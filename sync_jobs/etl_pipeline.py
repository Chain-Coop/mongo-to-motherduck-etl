import os
import logging
from pymongo import MongoClient
from urllib.parse import quote_plus
import duckdb
import pandas as pd
from dotenv import load_dotenv
import datetime
from config import COLLECTIONS, DATABASE_NAME, LOCAL_DUCKDB_PATH

def sync_bitcoinwallets(collection_name):
    PASSWORD = os.getenv("PASSWORD")
    MONGO_URI = os.getenv("MONGO_URI")
    MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

    if not PASSWORD:
        raise ValueError("❌ MongoDB password not found in environment variables")
    if not MONGO_URI:
        raise ValueError("❌ Mongo URI not found in environment variables")
    if not MOTHERDUCK_TOKEN:
        raise ValueError("❌ MOTHERDUCK_TOKEN not found in environment variables")

    # Improved password encoding with error handling
    try:
        encoded_password = quote_plus(PASSWORD.encode('utf-8'))  # Explicit encoding
    except Exception as e:
        raise ValueError(f"❌ Failed to encode password: {e}")

    mongo_uri = f"mongodb+srv://chosen:{encoded_password}@chain-co.c1mmgxn.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(mongo_uri) 
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    database = client.get_database("chain-co-dev")

    try:
        collection = database.get_collection(collection_name)
        data = list(collection.find())
        df = pd.DataFrame(data)
        print(df.dtypes)
        con = duckdb.connect()
        con.register('my_test_table', df)

        table_name = f"main_{collection_name}"
        try:
            con.execute("INSTALL motherduck; LOAD motherduck;")
            con.execute(f"SET motherduck_token='{MOTHERDUCK_TOKEN}'")
            con.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

            try:
                con.execute(f"ATTACH 'md:{DATABASE_NAME}'")
                print(f"Database attached successfully for {collection_name}")
            except Exception as attach_error:
                print(f"Database is already attached for {collection_name}: {attach_error}")
            
            con.execute(f"USE {DATABASE_NAME}")
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM my_test_table")
        except Exception as e:
            print(f"Error during table creation for {collection_name}: {e}")

        # Verify the data in MotherDuck
        result_motherduck = con.execute(f'SELECT * FROM {table_name}').fetchdf()
        print(result_motherduck.head(3))

    except Exception as e:
        print(f"Error fetching data for {collection_name}: {e}")

