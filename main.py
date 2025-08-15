import logging
import os
from sync_jobs.etl_pipeline import sync_bitcoinwallets
from config import COLLECTIONS
# from syn_test_env.bitcoinwallet import table_exists
logger = logging.getLogger("sync_logger")
logger.setLevel(logging.INFO)  # Make sure it's INFO or DEBUG
file_handler = logging.FileHandler("sync_log.txt")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
if __name__ == "__main__":
    # Explicitly check for required variables
    required_vars = ['PASSWORD', 'MONGO_URI', 'MOTHERDUCK_TOKEN']
    missing_vars = [var for var in required_vars if var not in os.environ]
    
    if missing_vars:
        error_msg = f"Missing environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise EnvironmentError(error_msg)
    logger.info("Starting the data pipeline...")

    # Loop through each collection and call the corresponding sync function
    for collection_name in COLLECTIONS:
        try:
            logger.info(f"Syncing {collection_name}...")
            if collection_name == "bitcoinwallets":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "users":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "contributionhistories":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "contributions":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "cashwyretransactions":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "lndwalletss":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "wallets":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "logs":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "manualsavings":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "web3histories":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "web3wallets":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "bvnlogs":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "vanttransactions":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
            if collection_name == "withdrawals":
                sync_bitcoinwallets(collection_name)  # Dynamically call the function for Bitcoin Wallets
            # Add more conditional blocks for other collections
            logger.info(f"✅ Successfully synced {collection_name}.")
        except Exception as e:
            logger.error(f"❌ Failed to sync {collection_name}: {e}")