from pymongo import MongoClient
from urllib.parse import quote_plus
import duckdb
import os
import pandas as pd
from bson import ObjectId  # Import ObjectId for comparison
from dotenv import load_dotenv

def c_contributions():
    load_dotenv()

    # Step 1: Encode MongoDB password
    password = os.getenv("password")
    encoded_password = quote_plus(password)

    # Step 2: MongoDB connection
    connection_string = f"mongodb+srv://chosen:{encoded_password}@chain-co.c1mmgxn.mongodb.net/?retryWrites=true&w=majority&appName=chain-co"
    client = MongoClient(connection_string)

    try:
        db = client["chain-co-dev"]
        collection = db["contributionhistories"]  # Corrected collection name
        print("✅ Successfully connected to MongoDB!")
    except Exception as e:
        print("❌ MongoDB Error:", e)
        return

    # Step 3: Check the total number of documents in MongoDB
    total_docs = collection.count_documents({})
    print(f"📊 Total logs in MongoDB: {total_docs}")

    # Step 4: Fetch documents from MongoDB
    docs_list = list(collection.find())

    # Step 5: If there are documents, proceed with sync logic
    if total_docs > 0:
        md_conn = duckdb.connect("md:my_db")

        # Step 6: Check if 'contributionhistories' table exists in DuckDB
        table_exists = md_conn.execute("""
            SELECT COUNT(*) 
            FROM duckdb_tables() 
            WHERE table_name = 'contributionhistories'
        """).fetchone()[0] > 0

        # Step 7: Pull records from MongoDB
        if table_exists:
            try:
                last_synced_contributions = md_conn.execute(
                    "SELECT mongo_id FROM contributionhistories ORDER BY mongo_id DESC LIMIT 1"
                ).fetchone()[0]
                print(f"🕒 Last synced mongo_id from DuckDB: {last_synced_contributions}")
            except Exception as e:
                print("⚠️ Failed to fetch last mongo_id from DuckDB:", e)
                last_synced_contributions = None

            if last_synced_contributions:
                # Convert last_synced_contributions to ObjectId for comparison
                last_synced_contributions = ObjectId(last_synced_contributions)  # Ensure it's an ObjectId
                docs = collection.find({"_id": {"$gt": last_synced_contributions}})
            else:
                docs = collection.find()
        else:
            docs = collection.find()

        # Step 8: Convert to DataFrame
        df = pd.DataFrame(docs_list)

        # ✅ Exit if Mongo returns no data
        if df.empty:
            print("ℹ️ No contributionhistories found in MongoDB.")
            return

        # Step 9: Convert _id and mongo_id
        if "_id" in df.columns:
            df["_id"] = df["_id"].astype(str)  # Convert _id to string
            df.rename(columns={"_id": "mongo_id"}, inplace=True)

        if "mongo_id" not in df.columns:
            raise ValueError("❌ Expected '_id' field not found in MongoDB documents.")

        # Step 10: Compare to existing MotherDuck records
        existing_ids = []
        try:
            existing_ids = md_conn.execute("SELECT mongo_id FROM contributionhistories").fetchdf()["mongo_id"].tolist()
        except duckdb.CatalogException:
            print("ℹ️ No existing contributionhistories table found. All records will be synced.")

        df = df[~df["mongo_id"].isin(existing_ids)]

        # ✅ Exit if no new records after deduplication
        if df.empty:
            print("ℹ️ No new contributions to sync.")
            return

        # Step 11: MotherDuck auth
        motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
        if not motherduck_token:
            raise ValueError("❌ MOTHERDUCK_TOKEN not set in .env file")

        duckdb.sql("INSTALL motherduck; LOAD motherduck;")
        duckdb.sql(f"SET motherduck_token='{motherduck_token}'")

        md_conn = duckdb.connect("md:my_db")  # reconnect with token

        # Step 12: Push data
        md_conn.register("df", df)
        md_conn.execute("CREATE TABLE IF NOT EXISTS contributionhistories AS SELECT * FROM df LIMIT 0")
        md_conn.execute("""
            INSERT INTO contributionhistories
            SELECT * FROM df
            WHERE mongo_id NOT IN (SELECT mongo_id FROM contributionhistories)
        """)

        print("✅ Data successfully pushed to MotherDuck!")
    else:
        print("ℹ️ No documents to sync from MongoDB.")
