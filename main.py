# import logging
# from dotenv import load_dotenv
# load_dotenv()
# import os
# from sync_jobs.contribution import sync_contributions
# from sync_jobs.c_history import c_contributions
# from sync_jobs.logs import sync_logsz
# from sync_jobs.manualsavings import sync_manualsavings
# from sync_jobs.user import sync_users
# from sync_jobs.wallet import sync_wallets
# from sync_jobs.web3histories import sync_web3histories
# from sync_jobs.web3wallets import sync_web3wallets
# from sync_jobs.cashwire import sync_cashwyretransactions
# from sync_jobs.ligtningWallet import sync_lndwallets
# from sync_jobs.bitcoinwallet import sync_bitcoinwallets

# # Set up the logger
# logger = logging.getLogger("sync_logger")
# logger.setLevel(logging.INFO)  # Set log level to INFO
# file_handler = logging.FileHandler("sync_log.txt")  # Log file name
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Log format
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

# if __name__ == "__main__":
#     logger.info("Starting the data pipeline...")

#     try:
#         logger.info("Syncing contributions...")
#         sync_contributions()
#         logger.info("✅ Successfully synced contributions.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync contributions: {e}")

#     try:
#         logger.info("Syncing contribution histories...")
#         c_contributions()
#         logger.info("✅ Successfully synced contribution histories.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync contribution histories: {e}")

#     try:
#         logger.info("Syncing logs...")
#         sync_logsz()
#         logger.info("✅ Successfully synced logs.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync logs: {e}")

#     # Repeat for other sync functions...
# import logging
# from dotenv import load_dotenv
# import os
# from pymongo import MongoClient
# import duckdb
# import pandas as pd
# from sync_jobs.contribution import sync_contributions
# from sync_jobs.c_history import c_contributions
# from sync_jobs.logs import sync_logsz
# from sync_jobs.manualsavings import sync_manualsavings
# from sync_jobs.user import sync_users
# from sync_jobs.wallet import sync_wallets
# from sync_jobs.web3histories import sync_web3histories
# from sync_jobs.web3wallets import sync_web3wallets
# from sync_jobs.cashwire import sync_cashwyretransactions
# from sync_jobs.ligtningWallet import sync_lndwallets
# from sync_jobs.bitcoinwallet import sync_bitcoinwallets

# # Set up the logger
# logger = logging.getLogger("sync_logger")
# logger.setLevel(logging.DEBUG)  # Set log level to DEBUG
# file_handler = logging.FileHandler("sync_log.txt")  # Log file name
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Log format
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

# def test_mongo_connection():
#     mongo_uri = os.getenv("MONGO_URI")
#     try:
#         client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
#         client.server_info()  # To confirm if MongoDB connection is successful
#         logger.info("✅ Successfully connected to MongoDB!")
#     except Exception as e:
#         logger.error(f"❌ Failed to connect to MongoDB: {e}")
#         raise e

# def sync_logsz():
#     try:
#         load_dotenv()  # Make sure environment variables are loaded
#         test_mongo_connection()  # Test MongoDB connection

#         mongo_uri = os.getenv("MONGO_URI")
#         client = MongoClient(mongo_uri)
#         db = client["chain-co-dev"]
#         collection = db["logs"]  # Collection you're syncing
#         logger.info("✅ Connected to MongoDB!")

#         # Fetch data from MongoDB
#         docs = list(collection.find())
#         logger.info(f"Fetched {len(docs)} records from MongoDB.")

#         # Convert data to DataFrame
#         if not docs:
#             logger.info("ℹ️ No data found in MongoDB.")
#             return

#         df = pd.DataFrame(docs)

#         # Log a preview of the data
#         logger.info(f"Data preview from MongoDB: {df.head()}")

#         # Check if DataFrame is empty
#         if df.empty:
#             logger.info("ℹ️ No data to insert into MotherDuck.")
#             return

#         # DuckDB / MotherDuck connection
#         md_conn = duckdb.connect("md:my_db")
#         logger.info("✅ Connected to MotherDuck!")

#         # Insert data into DuckDB
#         logger.info("Inserting data into MotherDuck...")
#         md_conn.register("df", df)
#         md_conn.execute("CREATE TABLE IF NOT EXISTS logs AS SELECT * FROM df LIMIT 0")
#         md_conn.execute("""
#             INSERT INTO logs
#             SELECT * FROM df
#             WHERE mongo_id NOT IN (SELECT mongo_id FROM logs)
#         """)

#         logger.info(f"✅ Successfully inserted {len(df)} records into MotherDuck.")

#     except Exception as e:
#         logger.error(f"❌ Failed to sync logs: {e}")
#         raise e


# if __name__ == "__main__":
#     logger.info("Starting data pipeline...")

#     try:
#         logger.info("Syncing contributions...")
#         sync_contributions()
#         logger.info("✅ Successfully synced contributions.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync contributions: {e}")

#     try:
#         logger.info("Syncing contribution histories...")
#         c_contributions()
#         logger.info("✅ Successfully synced contribution histories.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync contribution histories: {e}")

#     try:
#         logger.info("Syncing logs...")
#         sync_logsz()
#         logger.info("✅ Successfully synced logs.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync logs: {e}")

#     try:
#         logger.info("Syncing manual savings...")
#         sync_manualsavings()
#         logger.info("✅ Successfully synced manual savings.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync manual savings: {e}")

#     try:
#         logger.info("Syncing users...")
#         sync_users()
#         logger.info("✅ Successfully synced users.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync users: {e}")

#     try:
#         logger.info("Syncing wallets...")
#         sync_wallets()
#         logger.info("✅ Successfully synced wallets.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync wallets: {e}")

#     try:
#         logger.info("Syncing web3 histories...")
#         sync_web3histories()
#         logger.info("✅ Successfully synced web3 histories.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync web3 histories: {e}")

#     try:
#         logger.info("Syncing web3 wallets...")
#         sync_web3wallets()
#         logger.info("✅ Successfully synced web3 wallets.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync web3 wallets: {e}")

#     try:
#         logger.info("Syncing cashwyre transactions...")
#         sync_cashwyretransactions()
#         logger.info("✅ Successfully synced cashwyre transactions.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync cashwyre transactions: {e}")

#     try:
#         logger.info("Syncing lightning wallets...")
#         sync_lndwallets()
#         logger.info("✅ Successfully synced lightning wallets.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync lightning wallets: {e}")

#     try:
#         logger.info("Syncing bitcoin wallets...")
#         sync_bitcoinwallets()
#         logger.info("✅ Successfully synced bitcoin wallets.")
#     except Exception as e:
#         logger.error(f"❌ Failed to sync bitcoin wallets: {e}")



import logging
from dotenv import load_dotenv
import os
from sync_jobs.contribution import sync_contributions

# Set up the logger
logger = logging.getLogger("sync_logger")
logger.setLevel(logging.DEBUG)  # Set log level to DEBUG
file_handler = logging.FileHandler("sync_log.txt")  # Log file name
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Log format
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == "__main__":
    logger.info("Starting the data pipeline...")

    try:
        logger.info("Syncing contributions...")
        sync_contributions()  # Call sync_contributions directly
        logger.info("✅ Successfully synced contributions.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contributions: {e}")
