from pymongo import MongoClient
from urllib.parse import quote_plus
import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
from bson import ObjectId  # ✅ Required for filtering by MongoDB _id

def sync_users():
    load_dotenv()

    # Step 1: Encode MongoDB password
    password = os.getenv("password")
    encoded_password = quote_plus(password)

    # Step 2: MongoDB connection
    connection_string = f"mongodb+srv://chosen:{encoded_password}@chain-co.c1mmgxn.mongodb.net/?retryWrites=true&w=majority&appName=chain-co"
    client = MongoClient(connection_string)

    try:
        db = client["chain-co-dev"]
        collection = db["users"]
        print("✅ Successfully connected to MongoDB!")
    except Exception as e:
        print("❌ MongoDB Error:", e)
        return

    # Optional debug: Show total count in MongoDB
    total_docs = collection.count_documents({})
    print(f"📊 Total documents in MongoDB 'users': {total_docs}")

    # Step 3: Connect to DuckDB / MotherDuck
    md_conn = duckdb.connect("md:my_db")

    # Step 4: Check if 'users' table exists
    table_exists = md_conn.execute("""
        SELECT COUNT(*) 
        FROM duckdb_tables() 
        WHERE table_name = 'users'
    """).fetchone()[0] > 0

    # Step 5: Pull records from MongoDB (using _id for proper incremental sync)
    if table_exists:
        last_synced = md_conn.execute(
            "SELECT mongo_id FROM users ORDER BY mongo_id DESC LIMIT 1"
        ).fetchone()
        
        if last_synced and last_synced[0]:
            last_id = ObjectId(last_synced[0])
            docs = collection.find({"_id": {"$gt": last_id}})
        else:
            docs = collection.find()
    else:
        docs = collection.find()

    # Step 6: Convert MongoDB documents to DataFrame
    df = pd.DataFrame(docs)

    # ✅ Exit if MongoDB returns no data
    if df.empty:
        print("ℹ️ No new users found in MongoDB.")
        return

    print(f"📥 Pulled new documents from MongoDB: {df.shape[0]}")

    # Step 7: Convert MongoDB _id to mongo_id (ensure it's a string for comparison)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
        df.rename(columns={"_id": "mongo_id"}, inplace=True)

    if "mongo_id" not in df.columns:
        raise ValueError("❌ Expected '_id' field not found in MongoDB documents.")

    # Step 8: Ensure membershipStatus is a string
    if "membershipStatus" in df.columns:
        df["membershipStatus"] = df["membershipStatus"].astype(str)  # Convert to string if it's not already

    # Step 9: Deduplicate by checking existing mongo_ids in DuckDB
    existing_ids = []
    try:
        existing_ids = md_conn.execute("SELECT mongo_id FROM users").fetchdf()["mongo_id"].tolist()
    except duckdb.CatalogException:
        print("ℹ️ No existing users table found. All records will be synced.")

    df = df[~df["mongo_id"].isin(existing_ids)]

    # ✅ Exit if no new records after deduplication
    if df.empty:
        print("ℹ️ No new users to sync.")
        return

    # Step 10: Authenticate MotherDuck
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        raise ValueError("❌ MOTHERDUCK_TOKEN not set in .env file")

    duckdb.sql("INSTALL motherduck; LOAD motherduck;")
    duckdb.sql(f"SET motherduck_token='{motherduck_token}'")

    md_conn = duckdb.connect("md:my_db")  # reconnect with token

    # Step 11: Push data to MotherDuck
    md_conn.register("df", df)
    md_conn.execute("CREATE TABLE IF NOT EXISTS users AS SELECT * FROM df LIMIT 0")
    md_conn.execute("""
        INSERT INTO users
        SELECT * FROM df
        WHERE mongo_id NOT IN (SELECT mongo_id FROM users)
    """)

    print("✅ User data successfully pushed to MotherDuck!")
