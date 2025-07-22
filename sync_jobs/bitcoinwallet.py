from pymongo import MongoClient
from urllib.parse import quote_plus
import duckdb
import os
import pandas as pd
from bson import ObjectId
import logging
from dotenv import load_dotenv

def sync_bitcoinwallets():
    # Load environment variables
    load_dotenv()
    
    # 1. Get credentials
    password = os.getenv("PASSWORD")
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    
    if not password:
        raise ValueError("❌ MongoDB password not found in .env")
    if not motherduck_token:
        raise ValueError("❌ MotherDuck token not found in .env")

    # 2. MongoDB Connection
    try:
        encoded_password = quote_plus(password)
        connection_string = f"mongodb+srv://chosen:{encoded_password}@chain-co.c1mmgxn.mongodb.net/?retryWrites=true&w=majority&appName=chain-co"
        client = MongoClient(connection_string)
        db = client["chain-co-dev"]
        collection = db["bitcoinwallets"]
        logging.info("✅ MongoDB connection established")
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise

    # 3. MotherDuck Setup
    try:
        duckdb.sql("INSTALL motherduck; LOAD motherduck;")
        duckdb.sql(f"SET motherduck_token='{motherduck_token}'")
        md_conn = duckdb.connect("md:my_db")
        logging.info("✅ MotherDuck authentication successful")
    except Exception as e:
        logging.error(f"❌ MotherDuck setup failed: {e}")
        raise

    # 4. Check existing data
    try:
        # Check if table exists
        table_exists = md_conn.execute(""" 
            SELECT COUNT(*) FROM duckdb_tables() 
            WHERE table_name = 'bitcoinwallets' 
        """).fetchone()[0] > 0
        
        # Get last synced ID if table exists
        last_id = None
        if table_exists:
            try:
                last_id = md_conn.execute(
                    "SELECT mongo_id FROM bitcoinwallets ORDER BY mongo_id DESC LIMIT 1"
                ).fetchone()[0]
                logging.info(f"🕒 Last synced ID: {last_id}")
            except Exception as e:
                logging.warning(f"⚠️ Couldn't get last ID: {e}")
        
        # Fetch new documents with proper type conversion
        query = {"_id": {"$gt": ObjectId(last_id)}} if last_id else {}
        docs = list(collection.find(query))

        if not docs:
            logging.info("ℹ️ No new documents to sync")
            return
        
        # Convert to DataFrame with explicit type handling
        df = pd.DataFrame(docs)
        df["_id"] = df["_id"].astype(str)
        df.rename(columns={"_id": "mongo_id"}, inplace=True)

        # Remove the __v field if it is not needed
        if "__v" in df.columns:
            df.drop(columns=["__v"], inplace=True)

        # Deduplication logic
        if table_exists:
            existing_ids = md_conn.execute(
                "SELECT mongo_id FROM bitcoinwallets"
            ).fetchdf()["mongo_id"].tolist()
            df = df[~df["mongo_id"].isin(existing_ids)]

        if df.empty:
            logging.info("ℹ️ No new records after deduplication")
            return

        # Create the table schema with explicit fields based on MongoDB document structure
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bitcoinwallets (
                mongo_id VARCHAR PRIMARY KEY,
                user VARCHAR,
                encryptedPrivateKey VARCHAR,
                encryptedWif VARCHAR,
                publicKey VARCHAR,
                address VARCHAR,
                walletType VARCHAR,
                lockedAmount FLOAT,
                lockedAmountSatoshis FLOAT,
                lockDuration FLOAT,
                isLocked BOOLEAN,
                createdAt TIMESTAMP,
                updatedAt TIMESTAMP,
                __v INTEGER
            )
        """
        md_conn.execute(create_table_query)
        logging.info(f"✅ Table schema created in MotherDuck")

        # Push data to MotherDuck
        md_conn.register("new_data", df)
        md_conn.execute("""
            INSERT INTO bitcoinwallets
            SELECT * FROM new_data
            WHERE mongo_id NOT IN (
                SELECT mongo_id FROM bitcoinwallets
            )
        """)
        
        logging.info(f"✅ Successfully synced {len(df)} new records")

    except Exception as e:
        logging.error(f"❌ Sync failed: {e}")
        raise
