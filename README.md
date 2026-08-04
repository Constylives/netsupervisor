# NetSupervisor

Système de supervision réseau (dashboard) en Python : supervision asynchrone
d'hôtes/services (TCP, HTTP, ICMP), API REST + WebSocket temps réel, et
interface graphique de bureau.

## Fonctionnalités

- Supervision concurrente de plusieurs hôtes sans bloquer (asyncio)
- Trois types de vérification : TCP (ouverture de socket), HTTP (requête via
  aiohttp), ICMP (ping réel via icmplib — nécessite les droits administrateur)
- Mesure de latence et historique des mesures par hôte
- API REST (FastAPI) : liste des hôtes, statut, historique, ajout/suppression
- Diffusion temps réel des mesures via WebSocket (`/ws`)
- Interface graphique PyQt6 : tableau avec indicateurs colorés (up/down),
  graphe de latence (pyqtgraph), formulaire d'ajout/suppression d'hôte
- Logs (fichier + console) côté API et côté GUI
- Suite de tests automatisés (pytest, 32 tests)

## Architecture

\```
netsupervisor/
├── core/
│   ├── models.py     # Host, Measurement, CheckType, Status
│   ├── monitor.py    # SupervisionEngine (asyncio) : checks TCP/HTTP/ICMP
│   └── store.py       # Stockage en mémoire (hôtes + historique)
├── api/
│   └── main.py        # API FastAPI (REST + WebSocket)
├── gui/
│   └── App.py          # Interface PyQt6 (client de l'API, via qasync)
├── tests/
│   ├── test_models.py
│   ├── test_store.py
│   ├── test_monitor.py
│   └── test_api.py
├── logs/               # Généré automatiquement au lancement
├── pytest.ini
└── requirements.txt
\```

La supervision (`core/`) est indépendante de l'API et de la GUI : elle peut
être testée et réutilisée seule. L'API expose cette supervision au réseau ;
la GUI consomme uniquement l'API (REST pour l'état initial et l'historique,
WebSocket pour les mises à jour temps réel), jamais le moteur directement.

## Installation

Prérequis : Python 3.10+.

\```powershell
git clone <url-du-depot> netsupervisor
cd netsupervisor
python -m venv venv
venv\Scripts\Activate.ps1          # Windows (PowerShell)
# source venv/bin/activate         # Linux / macOS

\``` dans powershell, 

pip install -r requirements.txt
\```

## Lancement

Deux composants à lancer séparément, dans deux terminaux (même venv activé
dans les deux) :

**1. L'API** (backend) :

\```powershell
uvicorn api.main:app --reload
\```

Disponible sur `http://127.0.0.1:8000`. Documentation interactive générée
automatiquement par FastAPI : `http://127.0.0.1:8000/docs`.

**2. L'interface graphique** :

\```powershell
python gui\App.py
\```

Se connecte automatiquement à l'API (`http://127.0.0.1:8000`) et affiche
l'état des hôtes en temps réel.

> **Ping ICMP** : pour que le check `icmp` fonctionne (et non pas échouer
> avec "Permission refusée"), lancer le terminal en mode administrateur avant de
> démarrer l'API.

## Tests

\```powershell
pytest -v
\```

32 tests couvrant :
- `test_models.py` — validation des règles métier (Host, Measurement)
- `test_store.py` — stockage, historique borné, snapshot
- `test_monitor.py` — checks TCP/HTTP réels contre des serveurs locaux,
  non-blocage de la boucle asyncio, ajout/suppression dynamique, diffusion
  WebSocket
- `test_api.py` — tous les endpoints REST (CRUD hôtes, statut, historique)

Les tests réseau utilisent des serveurs TCP/HTTP éphémères lancés localement
par les tests eux-mêmes (pas de dépendance à une connexion internet).

## Endpoints API principaux

| Méthode | Route                        | Description                          |
|---------|-------------------------------|---------------------------------------|
| GET     | `/health`                     | Vérification de vie de l'API         |
| GET     | `/hosts`                      | Liste des hôtes supervisés           |
| POST    | `/hosts`                      | Ajouter un hôte                      |
| GET     | `/hosts/{id}`                 | Détail d'un hôte                     |
| DELETE  | `/hosts/{id}`                 | Supprimer un hôte                    |
| GET     | `/hosts/{id}/status`          | Statut courant + dernière mesure     |
| GET     | `/hosts/{id}/history?limit=N` | Historique des N dernières mesures   |
| GET     | `/status`                     | Instantané complet de tous les hôtes |
| WS      | `/ws`                         | Flux temps réel de chaque mesure     |

## Logs

- `logs/gui.log` — journal de l'interface graphique
- Logs de l'API affichés dans le terminal `uvicorn` (démarrage/arrêt du
  moteur, ajout/suppression d'hôtes, alertes de disponibilité)
