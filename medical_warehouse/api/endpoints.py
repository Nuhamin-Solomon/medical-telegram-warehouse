# api/schemas.py
from pydantic import BaseModel
from typing import List

class ProductStat(BaseModel):
    product_name: str
    product_category: str
    count: int

class ChannelActivity(BaseModel):
    channel_name: str
    message_count: int

class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
# api/endpoints.py
from fastapi import FastAPI
from typing import List
from .schemas import ProductStat, ChannelActivity, MessageSearchResult
import psycopg2
import os

app = FastAPI()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

@app.get("/top-products", response_model=List[ProductStat])
def top_products():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        select product_name, product_category, count(*) as count
        from yolo_detections
        group by product_name, product_category
        order by count desc
        limit 10;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [ProductStat(product_name=r[0], product_category=r[1], count=r[2]) for r in rows]
