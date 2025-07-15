from pymongo import MongoClient
from urllib.parse import quote_plus
import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
from bson import ObjectId  # Import ObjectId for comparison

def sync_logsz():
    print("👀 sync_logsz() was called")
    load_dotenv()

    # Step 1: Encode MongoDB password
    password = os.getenv("password")
    if not password:
        raise ValueError("❌ MongoDB password not found in .env")
    encoded_password = quote_plus(password)

    # Step 2: MongoDB connection
    connection_string = f"mongodb+srv://chosen:{encoded_password}@chain-co.c1mmgxn.mongodb.net/?retryWrites=true&w=majority&appName=chain-co"
    client = MongoClient(connection_string)

    try:
        db = client["chain-co-dev"]
        collection = db["logs"]
        print("✅ Successfully connected to MongoDB!")
    except Exception as e:
        print("❌ MongoDB Error:", e)
        return

    print(f"📊 Total logs in MongoDB: {collection.count_documents({})}")

    # Step 3: Connect to DuckDB / MotherDuck
    md_conn = duckdb.connect("md:my_db")

    # Step 4: Check if 'logs' table exists
    table_exists = md_conn.execute(""" 
        SELECT COUNT(*) 
        FROM duckdb_tables() 
        WHERE table_name = 'logs' 
    """).fetchone()[0] > 0

    # Step 5: Pull records from MongoDB
    if table_exists:
        try:
            last_synced_mongo_id = md_conn.execute(
                "SELECT mongo_id FROM logs ORDER BY mongo_id DESC LIMIT 1"
            ).fetchone()[0]
            print(f"🕒 Last synced mongo_id from DuckDB: {last_synced_mongo_id}")
        except Exception as e:
            print("⚠️ Failed to fetch last mongo_id from DuckDB:", e)
            last_synced_mongo_id = None

        # Debugging: Print out last synced mongo_id
        print(f"Last synced mongo_id: {last_synced_mongo_id}")
        
        if last_synced_mongo_id:
            # Convert last_synced_mongo_id to ObjectId for comparison
            last_synced_mongo_id = ObjectId(last_synced_mongo_id)  # Ensure we have an ObjectId for comparison
            docs = collection.find({"_id": {"$gt": last_synced_mongo_id}})
        else:
            docs = collection.find()
    else:
        docs = collection.find()

    # Debugging: Fetch and print the first document's mongo_id
    docs_list = list(docs)
    if docs_list:
        print(f"First document's mongo_id: {docs_list[0]['_id']}")
    print(f"📦 Documents pulled after filter: {len(docs_list)}")

    # Step 6: Convert to DataFrame
    df = pd.DataFrame(docs_list)

    # ✅ Exit if Mongo returns no data
    if df.empty:
        print("ℹ️ No logs found in MongoDB.")
        return

    # Step 7: Convert _id and mongo_id
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)  # Ensure _id is treated as a string
        df.rename(columns={"_id": "mongo_id"}, inplace=True)

    if "mongo_id" not in df.columns:
        raise ValueError("❌ Expected '_id' field not found in MongoDB documents.")

    # Step 8: Compare to existing MotherDuck records
    existing_ids = []
    try:
        existing_ids = md_conn.execute("SELECT mongo_id FROM logs").fetchdf()["mongo_id"].tolist()
    except duckdb.CatalogException:
        print("ℹ️ No existing logs table found. All records will be synced.")

    df = df[~df["mongo_id"].isin(existing_ids)]

    # ✅ Exit if no new records after deduplication
    if df.empty:
        print("ℹ️ No new logs to sync.")
        return

    # Step 9: MotherDuck auth
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        raise ValueError("❌ MOTHERDUCK_TOKEN not set in .env file")

    duckdb.sql("INSTALL motherduck; LOAD motherduck;")
    duckdb.sql(f"SET motherduck_token='{motherduck_token}'")

    md_conn = duckdb.connect("md:my_db")  # reconnect with token

    # Step 10: Push data
    md_conn.register("df", df)
    md_conn.execute("CREATE TABLE IF NOT EXISTS logs AS SELECT * FROM df LIMIT 0")
    md_conn.execute("""
        INSERT INTO logs
        SELECT * FROM df
        WHERE mongo_id NOT IN (SELECT mongo_id FROM logs)
    """)

    print("✅ Data successfully pushed to MotherDuck!")
