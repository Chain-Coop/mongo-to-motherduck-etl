# from pymongo import MongoClient
# from urllib.parse import quote_plus
# import duckdb
# import os
# import pandas as pd
# from dotenv import load_dotenv
# from bson import ObjectId  # Import ObjectId for MongoDB

# def sync_contributions():
#     load_dotenv()  # Make sure this is here!
    
#     # Step 1: Encode MongoDB password
#     PASSWORD = os.getenv("PASSWORD")
#     if not PASSWORD:
#         raise ValueError("❌ MongoDB password not found in .env")
#     encoded_password = quote_plus(PASSWORD)

#     # Step 2: Connect to MongoDB
#     connection_string = f"mongodb+srv://chosen:{encoded_password}@chain-co.c1mmgxn.mongodb.net/?retryWrites=true&w=majority&appName=chain-co"
#     client = MongoClient(connection_string)

#     try:
#         db = client["chain-co-dev"]
#         collection = db["contributions"]
#         print("✅ Successfully connected to MongoDB!")
#     except Exception as e:
#         print("❌ MongoDB Error:", e)
#         return

#     # Step 3: Connect to DuckDB / MotherDuck
#     md_conn = duckdb.connect("md:my_db")

#     # Step 4: Check if 'contributions' table exists in DuckDB
#     table_exists = md_conn.execute("""
#         SELECT COUNT(*) 
#         FROM duckdb_tables() 
#         WHERE table_name = 'contributions'
#     """).fetchone()[0] > 0

#     # Step 5: Pull data from MongoDB
#     if table_exists:
#         try:
#             # Get the last synced mongo_id from DuckDB
#             last_synced_contributions = md_conn.execute(
#                 "SELECT mongo_id FROM contributions ORDER BY mongo_id DESC LIMIT 1"
#             ).fetchone()[0]
#             print(f"🕒 Last synced mongo_id from DuckDB: {last_synced_contributions}")
#         except Exception as e:
#             print("⚠️ Failed to fetch last mongo_id from DuckDB:", e)
#             last_synced_contributions = None

#         # Fetch documents from MongoDB with a filter based on mongo_id
#         if last_synced_contributions:
#             last_synced_contributions = ObjectId(last_synced_contributions)  # Convert to ObjectId
#             docs = collection.find({"_id": {"$gt": last_synced_contributions}})
#         else:
#             docs = collection.find()
#     else:
#         docs = collection.find()

#     # Step 6: Convert MongoDB documents to DataFrame
#     df = pd.DataFrame(docs)

#     # ✅ Exit if MongoDB returns no data
#     if df.empty:
#         print("ℹ️ No contributions found in MongoDB.")
#         return

#     # Step 7: Convert MongoDB _id to mongo_id (ensure it's a string for comparison)
#     if "_id" in df.columns:
#         df["_id"] = df["_id"].astype(str)  # Convert _id to string
#         df.rename(columns={"_id": "mongo_id"}, inplace=True)

#     if "mongo_id" not in df.columns:
#         raise ValueError("❌ Expected '_id' field not found in MongoDB documents.")

#     # Step 8: Deduplicate by checking existing mongo_ids in DuckDB
#     existing_ids = []
#     try:
#         existing_ids = md_conn.execute("SELECT mongo_id FROM contributions").fetchdf()["mongo_id"].tolist()
#     except duckdb.CatalogException:
#         print("ℹ️ No existing contributions table found. All records will be synced.")

#     df = df[~df["mongo_id"].isin(existing_ids)]

#     # ✅ Exit if no new records after deduplication
#     if df.empty:
#         print("ℹ️ No new contributions to sync.")
#         return

#     # Step 9: Authenticate MotherDuck
#     motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
#     if not motherduck_token:
#         raise ValueError("❌ MOTHERDUCK_TOKEN not set in .env file")

#     duckdb.sql("INSTALL motherduck; LOAD motherduck;")
#     duckdb.sql(f"SET motherduck_token='{motherduck_token}'")

#     md_conn = duckdb.connect("md:my_db")  # reconnect with token

#     # Step 10: Push data to MotherDuck
#     md_conn.register("df", df)
#     md_conn.execute("CREATE TABLE IF NOT EXISTS contributions AS SELECT * FROM df LIMIT 0")
#     md_conn.execute("""
#         INSERT INTO contributions
#         SELECT * FROM df
#         WHERE mongo_id NOT IN (SELECT mongo_id FROM contributions)
#     """)

#     print("✅ 33 contributions successfully pushed to MotherDuck!")


from pymongo import MongoClient
from urllib.parse import quote_plus
import duckdb
import os
import pandas as pd
from bson import ObjectId  # Import ObjectId for MongoDB

def sync_contributions():
    # Retrieve MongoDB password and MotherDuck token from environment variables
    PASSWORD = os.getenv("PASSWORD")
    MONGO_URI = os.getenv("MONGO_URI")
    MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

    if not PASSWORD:
        raise ValueError("❌ MongoDB password not found in environment variables")
    if not MONGO_URI:
        raise ValueError("❌ Mongo URI not found in environment variables")
    if not MOTHERDUCK_TOKEN:
        raise ValueError("❌ MOTHERDUCK_TOKEN not found in environment variables")

    # Encode MongoDB password
    encoded_password = quote_plus(PASSWORD)

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client["chain-co-dev"]
    collection = db["contributions"]

    # Connect to DuckDB / MotherDuck
    md_conn = duckdb.connect("md:my_db")

    # Check if 'contributions' table exists in DuckDB
    table_exists = md_conn.execute("""
        SELECT COUNT(*) 
        FROM duckdb_tables() 
        WHERE table_name = 'contributions'
    """).fetchone()[0] > 0

    # Fetch data from MongoDB
    if table_exists:
        try:
            last_synced_contributions = md_conn.execute(
                "SELECT mongo_id FROM contributions ORDER BY mongo_id DESC LIMIT 1"
            ).fetchone()[0]
            print(f"🕒 Last synced mongo_id from DuckDB: {last_synced_contributions}")
        except Exception as e:
            print("⚠️ Failed to fetch last mongo_id from DuckDB:", e)
            last_synced_contributions = None

        # Fetch documents from MongoDB with a filter based on mongo_id
        if last_synced_contributions:
            last_synced_contributions = ObjectId(last_synced_contributions)  # Convert to ObjectId
            docs = collection.find({"_id": {"$gt": last_synced_contributions}})
        else:
            docs = collection.find()
    else:
        docs = collection.find()

    # Convert MongoDB documents to DataFrame
    df = pd.DataFrame(docs)

    # Exit if MongoDB returns no data
    if df.empty:
        print("ℹ️ No contributions found in MongoDB.")
        return

    # Convert MongoDB _id to mongo_id
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
        df.rename(columns={"_id": "mongo_id"}, inplace=True)

    if "mongo_id" not in df.columns:
        raise ValueError("❌ Expected '_id' field not found in MongoDB documents.")

    # Deduplicate by checking existing mongo_ids in DuckDB
    existing_ids = []
    try:
        existing_ids = md_conn.execute("SELECT mongo_id FROM contributions").fetchdf()["mongo_id"].tolist()
    except duckdb.CatalogException:
        print("ℹ️ No existing contributions table found. All records will be synced.")

    df = df[~df["mongo_id"].isin(existing_ids)]

    # Exit if no new records after deduplication
    if df.empty:
        print("ℹ️ No new contributions to sync.")
        return

    # Authenticate MotherDuck
    duckdb.sql("INSTALL motherduck; LOAD motherduck;")
    duckdb.sql(f"SET motherduck_token='{MOTHERDUCK_TOKEN}'")

    md_conn = duckdb.connect("md:my_db")  # reconnect with token

    # Push data to MotherDuck
    md_conn.register("df", df)
    md_conn.execute("CREATE TABLE IF NOT EXISTS contributions AS SELECT * FROM df LIMIT 0")
    md_conn.execute("""
        INSERT INTO contributions
        SELECT * FROM df
        WHERE mongo_id NOT IN (SELECT mongo_id FROM contributions)
    """)

    print(f"✅ Successfully pushed {len(df)} contributions to MotherDuck!")
