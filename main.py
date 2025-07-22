import logging
import os
from sync_jobs.contribution import sync_contributions
from sync_jobs.c_history import c_contributions
from sync_jobs.bitcoinwallet import sync_bitcoinwallets

# Set up the logger
logger = logging.getLogger("sync_logger")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("sync_log.txt")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == "__main__":
    # Debug: Show all environment variables (remove after debugging)
    logger.info(f"Current environment variables: {dict(os.environ)}")
    
    # Explicitly check for required variables
    required_vars = ['PASSWORD', 'MONGO_URI', 'MOTHERDUCK_TOKEN']
    missing_vars = [var for var in required_vars if var not in os.environ]
    
    if missing_vars:
        error_msg = f"Missing environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise EnvironmentError(error_msg)
    
    logger.info("Starting the data pipeline...")
    try:
        logger.info("Syncing contributions...")
        sync_contributions()
        logger.info("✅ Successfully synced contributions.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contributions: {e}")
    try:
        logger.info("contributions list...")
        c_contributions()
        logger.info("✅ Successfully synced contributions history.")
    except Exception as e:
        logger.error(f"❌ Failed to sync contributions history: {e}")
    try:
        logger.info("bitcoinwallet list...")
        sync_bitcoinwallets()
        logger.info("✅ Successfully synced bitcoin wallet.")
    except Exception as e:
        logger.error(f"❌ Failed to sync bitcoin wallet: {e}")