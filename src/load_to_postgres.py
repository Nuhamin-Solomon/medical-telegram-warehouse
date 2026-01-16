import os
import json
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Folder containing your raw JSON data
DATA_DIR = "data/raw/telegram_messages"

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
except psycopg2.OperationalError as e:
    print(f"❌ Could not connect to database: {e}")
    exit(1)

cur = conn.cursor()

# Create schema and table
cur.execute("""
CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.telegram_messages;

CREATE TABLE raw.telegram_messages (
    message_id BIGINT,
    channel_name TEXT,
    message_date TIMESTAMP,
    message_text TEXT,
    views INTEGER,
    forwards INTEGER,
    has_media BOOLEAN,
    image_path TEXT
);
""")

conn.commit()

# Insert data into table
insert_sql = """
INSERT INTO raw.telegram_messages VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""

for root, _, files in os.walk(DATA_DIR):
    for file in files:
        if file.endswith(".json"):
            full_path = os.path.join(root, file)
            with open(full_path, "r", encoding="utf-8") as f:
                try:
                    records = json.load(f)
                except json.JSONDecodeError:
                    print(f"⚠️ Could not decode JSON file: {full_path}")
                    continue

            for r in records:
                cur.execute(insert_sql, (
                    r.get("message_id"),
                    r.get("channel_name"),
                    r.get("message_date"),
                    r.get("message_text"),
                    r.get("views"),
                    r.get("forwards"),
                    r.get("has_media"),
                    r.get("image_path")
                ))

# Close connection
conn.commit()
cur.close()
conn.close()

print("✅ Raw telegram data loaded into PostgreSQL")
