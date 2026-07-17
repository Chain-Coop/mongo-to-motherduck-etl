````markdown
# chaincoop-data-sync

> Automated MongoDB to MotherDuck (DuckDB Cloud) sync pipeline for ChainCoop — syncing cooperative financial data on a daily schedule via GitHub Actions.

---

## Architecture

```
MongoDB (Atlas)                  MotherDuck (DuckDB Cloud)
────────────────                 ──────────────────────────
  bitcoinwallets      ──►
  users               ──►
  contributions       ──►
  contributionhistories ──►       ChainCoop Analytics DB
  cashwyretransactions ──►
  wallets             ──►
  withdrawals         ──►
  logs                ──►
  + more collections  ──►

GitHub Actions orchestrates daily sync at 6:00 AM UTC
```

---

## Stack

| Layer | Technology |
|---|---|
| Source | MongoDB |
| Orchestration | GitHub Actions |
| Transformation | Python |
| Warehouse | DuckDB + MotherDuck |

---

## Collections Synced

| Collection | Description |
|---|---|
| `bitcoinwallets` | Bitcoin wallet records |
| `users` | Cooperative member accounts |
| `contributions` | Member contribution records |
| `contributionhistories` | Historical contribution logs |
| `cashwyretransactions` | Cashwyre transaction records |
| `wallets` | General wallet data |
| `withdrawals` | Withdrawal records |
| `lndwalletss` | Lightning Network wallet data |
| `manualsavings` | Manual savings entries |
| `web3histories` | Web3 transaction history |
| `web3wallets` | Web3 wallet records |
| `bvnlogs` | BVN verification logs |
| `vanttransactions` | Vant transaction records |
| `logs` | System logs |

---

## Repository Structure

```
chaincoop-data-sync/
├── .github/workflows/
│   └── sync.yml              # GitHub Actions daily sync workflow
├── sync_jobs/
│   └── etl_pipeline.py       # Core ETL logic per collection
├── config.py                 # Collection names and config
├── main.py                   # Pipeline entry point
├── requirements.txt
└── README.md
```

---

## Data Flow

```
┌─────────────────────────────────────┐
│         GitHub Actions              │
│   Scheduled daily at 6:00 AM UTC    │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│           MongoDB                   │
│   (ChainCoop production database)   │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│   ETL Pipeline (Python)             │
│   Extract → Transform → Load        │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│         MotherDuck                  │
│   (DuckDB Cloud Analytics Warehouse)│
└─────────────────────────────────────┘
```

---

## Key Features

- **Daily automated sync** — runs every day at 6:00 AM UTC via GitHub Actions
- **Multi-collection support** — syncs 14 MongoDB collections in a single run
- **Persistent logging** — all sync activity written to `sync_log.txt` and committed back to the repo
- **Error handling** — failed collections are logged and skipped without stopping the full pipeline
- **Manual trigger** — pipeline can be triggered on demand via `workflow_dispatch`

---

## Local Development

```bash
# Clone
git clone https://github.com/YOUR_ORG/chaincoop-data-sync.git
cd chaincoop-data-sync

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MONGO_URI=your_mongodb_connection_string
export PASSWORD=your_motherduck_password
export MOTHERDUCK_TOKEN=your_motherduck_token

# Run the pipeline
python main.py
```

---

## GitHub Actions Secrets

Add these to your repository under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `MONGO_URI` | MongoDB connection string |
| `PASSWORD` | Database password |
| `MOTHERDUCK_TOKEN` | MotherDuck authentication token |

---

## Schedule

| Workflow | Schedule (UTC) | Description |
|---|---|---|
| `sync.yml` | 6:00 AM daily | Full MongoDB → MotherDuck sync |

---

## License

MIT
````
