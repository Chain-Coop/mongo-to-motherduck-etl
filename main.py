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
    try:
        sync_contributions()
        logger.info("✅ Successfully synced contributions.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contributions: {e}")

    try:
        c_contributions()
        logger.info("✅ Successfully synced contribution histories.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contribution histories: {e}")

    try:
        sync_logsz()
        logger.info("✅ Successfully synced logs.")
    except Exception as e:
        logger.error(f"❌ Failed to sync logs: {e}")

    try:
        sync_manualsavings()
        logger.info("✅ Successfully synced manual savings.")
    except Exception as e:
        logger.error(f"❌ Failed to sync manual savings: {e}")

    try:
        sync_users()
        logger.info("✅ Successfully synced users.")
    except Exception as e:
        logger.error(f"❌ Failed to sync users: {e}")

    try:
        sync_wallets()
        logger.info("✅ Successfully synced wallets.")
    except Exception as e:
        logger.error(f"❌ Failed to sync wallets: {e}")

    try:
        sync_web3histories()
        logger.info("✅ Successfully synced web3 histories.")
    except Exception as e:
        logger.error(f"❌ Failed to sync web3 histories: {e}")

    try:
        sync_web3wallets()
        logger.info("✅ Successfully synced web3 wallets.")
    except Exception as e:
        logger.error(f"❌ Failed to sync web3 wallets: {e}")

    try:
        sync_cashwyretransactions()
        logger.info("✅ Successfully synced cashwyre transactions.")
    except Exception as e:
        logger.error(f"❌ Failed to sync cashwyre transactions: {e}")

    try:
        sync_lndwallets()
        logger.info("✅ Successfully synced lightning wallets.")
    except Exception as e:
        logger.error(f"❌ Failed to sync lightning wallets: {e}")

    try:
        sync_bitcoinwallets()
        logger.info("✅ Successfully synced bitcoin wallets.")
    except Exception as e:
        logger.error(f"❌ Failed to sync bitcoin wallets: {e}")
