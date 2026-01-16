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

RAW_DIR = "data/raw/telegram_messages"

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = conn.cursor()

cursor.execute("""
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id BIGINT,
    channel_name TEXT,
    message_date TIMESTAMP,
    message_text TEXT,
    views INT,
    forwards INT,
    has_media BOOLEAN,
    image_path TEXT
);
""")

conn.commit()

def load_json_files():
    for root, _, files in os.walk(RAW_DIR):
        for file in files:
            if file.endswith(".json"):
                path = os.path.join(root, file)
                with open(path, "r", encoding="utf-8") as f:
                    messages = json.load(f)

                for msg in messages:
                    cursor.execute("""
                        INSERT INTO raw.telegram_messages VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        msg.get("message_id"),
                        msg.get("channel_name"),
                        msg.get("message_date"),
                        msg.get("message_text"),
                        msg.get("views"),
                        msg.get("forwards"),
                        msg.get("has_media"),
                        msg.get("image_path"),
                    ))

                conn.commit()
                print(f"Loaded {file}")

if __name__ == "__main__":
    load_json_files()
    cursor.close()
    conn.close()
