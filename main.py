import logging
from dotenv import load_dotenv
load_dotenv()
import os
from sync_jobs.contribution import sync_contributions
from sync_jobs.c_history import c_contributions
from sync_jobs.logs import sync_logsz
from sync_jobs.manualsavings import sync_manualsavings
from sync_jobs.user import sync_users
from sync_jobs.wallet import sync_wallets
from sync_jobs.web3histories import sync_web3histories
from sync_jobs.web3wallets import sync_web3wallets
from sync_jobs.cashwire import sync_cashwyretransactions
from sync_jobs.ligtningWallet import sync_lndwallets
from sync_jobs.bitcoinwallet import sync_bitcoinwallets

# Set up the logger
logger = logging.getLogger("sync_logger")
logger.setLevel(logging.INFO)  # Set log level to INFO
file_handler = logging.FileHandler("sync_log.txt")  # Log file name
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Log format
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == "__main__":
    logger.info("Starting the data pipeline...")

    try:
        logger.info("Syncing contributions...")
        sync_contributions()
        logger.info("✅ Successfully synced contributions.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contributions: {e}")

    try:
        logger.info("Syncing contribution histories...")
        c_contributions()
        logger.info("✅ Successfully synced contribution histories.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contribution histories: {e}")

    try:
        logger.info("Syncing logs...")
        sync_logsz()
        logger.info("✅ Successfully synced logs.")
    except Exception as e:
        logger.error(f"❌ Failed to sync logs: {e}")

    # Repeat for other sync functions...
