import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATA_DIR = "data/raw/telegram_messages"

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

# Create analytics schema and table
cur.execute("""
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.telegram_messages;

CREATE TABLE analytics.telegram_messages (
    message_id BIGINT PRIMARY KEY,
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

# Insert all JSON records
insert_sql = """
INSERT INTO analytics.telegram_messages 
(message_id, channel_name, message_date, message_text, views, forwards, has_media, image_path)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (message_id) DO NOTHING
"""

for root, _, files in os.walk(DATA_DIR):
    for file in files:
        if file.endswith(".json"):
            full_path = os.path.join(root, file)
            with open(full_path, "r", encoding="utf-8") as f:
                records = json.load(f)

            for r in records:
                cur.execute(insert_sql, (
                    r["message_id"],
                    r["channel_name"],
                    r["message_date"],
                    r["message_text"],
                    r["views"],
                    r["forwards"],
                    r["has_media"],
                    r["image_path"]
                ))

conn.commit()
cur.close()
conn.close()

print("✅ Raw telegram data loaded into PostgreSQL (analytics.telegram_messages)")
