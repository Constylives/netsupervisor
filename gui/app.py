"""Interface graphique PyQt6 - client de l'API NetSupervisor.

Récupère l'état initial via REST (GET /status) puis reçoit les mises à
jour en temps réel via WebSocket (/ws). Utilise qasync pour faire
cohabiter la boucle Qt et asyncio sans jamais bloquer l'interface.

Ajout : graphe de latence (pyqtgraph) pour l'hôte sélectionné dans le
tableau, alimenté par l'historique REST au clic puis mis à jour en
direct via le flux WebSocket.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
import pyqtgraph as pg
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QPushButton, QLineEdit, QComboBox,
    QLabel, QMessageBox, QFormLayout, QGroupBox,
)
import qasync
from qasync import asyncSlot

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/gui.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("netsupervisor.gui")

API_BASE = "http://127.0.0.1:8000"
WS_URL = "ws://127.0.0.1:8000/ws"

COLUMNS = ["ID", "Nom", "Adresse", "Type", "Statut", "Latence (ms)", "Erreur", "Dernière maj"]
STATUS_COLORS = {
    "up": QColor(6, 64, 43),      # vert foncé
    "down": QColor(139, 0, 0),    # rouge foncé
    "unknown": QColor(230, 230, 230),  # gris
}
MAX_POINTS = 50  # nombre de points conservés dans le graphe de latence


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("NetSupervisor - Dashboard")
        self.resize(1100, 750)

        self.row_by_host: Dict[str, int] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self._ws_task: Optional[asyncio.Task] = None

        self.selected_host_id: Optional[str] = None
        self.latency_series: Dict[str, List[Optional[float]]] = {}

        self._build_ui()

    # ---------------------- UI ----------------------

    def _build_ui(self):
        central = QWidget()
        layout = QVBoxLayout(central)

        self.status_label = QLabel("Connexion en cours...")
        layout.addWidget(self.status_label)

        self.table = QTableWidget(0, len(COLUMNS))
        self.table.setHorizontalHeaderLabels(COLUMNS)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.itemSelectionChanged.connect(self.on_row_selected)
        layout.addWidget(self.table, 3)

        graph_box = QGroupBox("Latence (hôte sélectionné)")
        graph_layout = QVBoxLayout()
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground("#1e1e1e")
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setLabel("left", "Latence (ms)")
        self.plot_widget.setLabel("bottom", "Mesures récentes")
        self.plot_widget.setMinimumHeight(150)
        self.plot_widget.setMaximumHeight(220)
        self.plot_curve = self.plot_widget.plot(
            [], [], pen=pg.mkPen(color="#00c8ff", width=2), symbol="o", symbolSize=5
        )
        graph_layout.addWidget(self.plot_widget)
        graph_box.setLayout(graph_layout)
        layout.addWidget(graph_box, 1)

        form_box = QGroupBox("Ajouter un hôte")
        form_layout = QFormLayout()

        self.in_id = QLineEdit()
        self.in_name = QLineEdit()
        self.in_address = QLineEdit()
        self.in_type = QComboBox()
        self.in_type.addItems(["tcp", "http", "icmp"])
        self.in_port = QLineEdit()
        self.in_port.setPlaceholderText("requis si TCP")
        self.in_url = QLineEdit()
        self.in_url.setPlaceholderText("requis si HTTP, ex: https://example.com")

        form_layout.addRow("ID:", self.in_id)
        form_layout.addRow("Nom:", self.in_name)
        form_layout.addRow("Adresse:", self.in_address)
        form_layout.addRow("Type:", self.in_type)
        form_layout.addRow("Port:", self.in_port)
        form_layout.addRow("URL:", self.in_url)
        form_box.setLayout(form_layout)
        layout.addWidget(form_box)

        btn_row = QHBoxLayout()
        self.btn_add = QPushButton("Ajouter l'hôte")
        self.btn_add.clicked.connect(self.on_add_clicked)
        self.btn_remove = QPushButton("Supprimer l'hôte sélectionné")
        self.btn_remove.clicked.connect(self.on_remove_clicked)
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_remove)
        layout.addLayout(btn_row)

        self.setCentralWidget(central)

    # ---------------------- Cycle de vie async ----------------------

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self.load_initial_status()
        self._ws_task = asyncio.create_task(self.listen_ws())

    async def shutdown(self):
        if self._ws_task:
            self._ws_task.cancel()
        if self.session:
            await self.session.close()
        logger.info("GUI fermée proprement")

    # ---------------------- REST ----------------------

    async def load_initial_status(self):
        try:
            async with self.session.get(f"{API_BASE}/status", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
            self.table.setRowCount(0)
            self.row_by_host.clear()
            for host_id, entry in data.items():
                self._ensure_row(host_id, entry["host"])
                self._apply_measurement(host_id, entry.get("last_measurement"), entry.get("status"))
            self.status_label.setText(f"Connecté à l'API - {len(data)} hôte(s) supervisé(s)")
            logger.info("État initial chargé (%d hôtes)", len(data))
        except Exception as e:
            self.status_label.setText(f"Erreur de connexion à l'API: {e}")
            logger.error("Impossible de charger l'état initial: %s", e)

    @asyncSlot()
    async def on_add_clicked(self):
        host_id = self.in_id.text().strip()
        name = self.in_name.text().strip()
        address = self.in_address.text().strip()
        check_type = self.in_type.currentText()
        port_text = self.in_port.text().strip()
        url = self.in_url.text().strip() or None

        if not host_id or not name or not address:
            QMessageBox.warning(self, "Champs manquants", "ID, Nom et Adresse sont obligatoires.")
            return

        payload = {
            "id": host_id, "name": name, "address": address,
            "check_type": check_type, "interval": 5.0, "timeout": 3.0,
        }
        if check_type == "tcp":
            if not port_text.isdigit():
                QMessageBox.warning(self, "Port invalide", "Un port numérique est requis pour un check TCP.")
                return
            payload["port"] = int(port_text)
        elif check_type == "http":
            if not url:
                QMessageBox.warning(self, "URL manquante", "Une URL est requise pour un check HTTP.")
                return
            payload["url"] = url

        try:
            async with self.session.post(f"{API_BASE}/hosts", json=payload,
                                          timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 201:
                    host = await resp.json()
                    self._ensure_row(host["id"], host)
                    logger.info("Hôte ajouté via GUI: %s", host["id"])
                    self.in_id.clear(); self.in_name.clear(); self.in_address.clear()
                    self.in_port.clear(); self.in_url.clear()
                else:
                    detail = (await resp.json()).get("detail", "erreur inconnue")
                    QMessageBox.critical(self, "Erreur API", detail)
        except Exception as e:
            QMessageBox.critical(self, "Erreur réseau", str(e))
            logger.error("Erreur lors de l'ajout d'hôte: %s", e)

    @asyncSlot()
    async def on_remove_clicked(self):
        row = self.table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Sélection requise", "Sélectionne une ligne à supprimer.")
            return
        host_id = self.table.item(row, 0).text()
        try:
            async with self.session.delete(f"{API_BASE}/hosts/{host_id}",
                                            timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 204:
                    self.table.removeRow(row)
                    self.row_by_host.pop(host_id, None)
                    self.latency_series.pop(host_id, None)
                    if self.selected_host_id == host_id:
                        self.selected_host_id = None
                        self.plot_curve.setData([], [])
                        self.plot_widget.setTitle("")
                    self._reindex_rows()
                    logger.info("Hôte supprimé via GUI: %s", host_id)
                else:
                    QMessageBox.critical(self, "Erreur API", f"Suppression échouée (HTTP {resp.status})")
        except Exception as e:
            QMessageBox.critical(self, "Erreur réseau", str(e))
            logger.error("Erreur lors de la suppression d'hôte: %s", e)

    # ---------------------- Sélection / historique / graphe ----------------------

    @asyncSlot()
    async def on_row_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        host_id = self.table.item(row, 0).text()
        self.selected_host_id = host_id
        await self.load_history(host_id)

    async def load_history(self, host_id: str):
        try:
            async with self.session.get(f"{API_BASE}/hosts/{host_id}/history", params={"limit": MAX_POINTS},
                                         timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            self.latency_series[host_id] = [m["latency_ms"] for m in data]
            self._refresh_plot()
        except Exception as e:
            logger.error("Erreur chargement historique %s: %s", host_id, e)

    def _refresh_plot(self):
        if not self.selected_host_id:
            return
        series = self.latency_series.get(self.selected_host_id, [])
        xs = list(range(len(series)))
        ys = [v if v is not None else 0 for v in series]
        self.plot_widget.setTitle(self.selected_host_id)
        self.plot_curve.setData(xs, ys)

    # ---------------------- WebSocket ----------------------

    async def listen_ws(self):
        while True:
            try:
                async with self.session.ws_connect(WS_URL, timeout=10) as ws:
                    self.status_label.setText("Connecté (temps réel actif)")
                    logger.info("WebSocket connecté")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._apply_measurement_raw(msg.json())
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("WebSocket déconnecté, tentative de reconnexion dans 3s: %s", e)
                self.status_label.setText("Reconnexion en cours...")
                await asyncio.sleep(3)

    # ---------------------- Mise à jour du tableau ----------------------

    def _ensure_row(self, host_id: str, host: dict):
        if host_id in self.row_by_host:
            return
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.row_by_host[host_id] = row
        self.table.setItem(row, 0, QTableWidgetItem(host_id))
        self.table.setItem(row, 1, QTableWidgetItem(host.get("name", "")))
        self.table.setItem(row, 2, QTableWidgetItem(host.get("address", "")))
        self.table.setItem(row, 3, QTableWidgetItem(host.get("check_type", "")))
        self.table.setItem(row, 4, QTableWidgetItem("unknown"))
        self.table.setItem(row, 5, QTableWidgetItem(""))
        self.table.setItem(row, 6, QTableWidgetItem(""))
        self.table.setItem(row, 7, QTableWidgetItem(""))

    def _apply_measurement(self, host_id: str, measurement: Optional[dict], status: Optional[str]):
        if host_id not in self.row_by_host:
            return
        row = self.row_by_host[host_id]
        status_val = (measurement or {}).get("status") or status or "unknown"
        latency = (measurement or {}).get("latency_ms")
        error = (measurement or {}).get("error")
        ts = (measurement or {}).get("timestamp", "")
        self._set_row_values(row, status_val, latency, error, ts)

    def _apply_measurement_raw(self, m: dict):
        host_id = m.get("host_id")
        if host_id not in self.row_by_host:
            return  # mesure d'un hôte ajouté ailleurs, pas encore visible ici
        row = self.row_by_host[host_id]
        self._set_row_values(row, m.get("status", "unknown"), m.get("latency_ms"), m.get("error"), m.get("timestamp", ""))

        series = self.latency_series.setdefault(host_id, [])
        series.append(m.get("latency_ms"))
        if len(series) > MAX_POINTS:
            series.pop(0)
        if host_id == self.selected_host_id:
            self._refresh_plot()

    def _set_row_values(self, row: int, status: str, latency, error, ts: str):
        self.table.setItem(row, 4, QTableWidgetItem(status))
        self.table.setItem(row, 5, QTableWidgetItem(f"{latency:.1f}" if latency is not None else ""))
        self.table.setItem(row, 6, QTableWidgetItem(error or ""))
        self.table.setItem(row, 7, QTableWidgetItem(self._format_ts(ts)))

        color = STATUS_COLORS.get(status, STATUS_COLORS["unknown"])
        for col in range(len(COLUMNS)):
            item = self.table.item(row, col)
            if item:
                item.setBackground(color)

    def _reindex_rows(self):
        self.row_by_host = {self.table.item(r, 0).text(): r for r in range(self.table.rowCount())}

    @staticmethod
    def _format_ts(ts: str) -> str:
        if not ts:
            return ""
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%H:%M:%S")
        except Exception:
            return ts


def main():
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = MainWindow()
    window.show()

    with loop:
        loop.create_task(window.start())
        app.aboutToQuit.connect(lambda: loop.create_task(window.shutdown()))
        loop.run_forever()


if __name__ == "__main__":
    main()