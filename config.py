import os
from dotenv import load_dotenv
load_dotenv()
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN") 
PASSWORD = os.getenv("PASSWORD") 

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Define collection names dynamically
COLLECTIONS = [
    "bitcoinwallets",
    "contributionhistories", 
    "cashwyretransactions",
    "contributions", 
    "lndwallets",
    "logs",
    "manualsavings",
    "users",
    "wallets",
    "web3histories",
    "web3wallets",
    "withdrawals",
    "vanttransactions",
    "bvnlogs"
]

# MotherDuck Database and Table
#DATABASE_NAME = "test_my_cloud_db"
DATABASE_NAME="production_db"
TABLE_PREFIX = "test_"

# DuckDB local path
LOCAL_DUCKDB_PATH = "test_my_duckdb.duckdb"
TABLE_NAME="test_bitcoinwallets"