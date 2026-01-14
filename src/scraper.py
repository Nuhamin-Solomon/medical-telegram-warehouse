import os
import json
import logging
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "medical_scraper")

# Channels to scrape
CHANNELS = [
    "https://t.me/lobelia4cosmetics",
    "https://t.me/tikvahpharma",
]

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Directories
RAW_DATA_DIR = "data/raw/telegram_messages"
IMAGE_DIR = "data/raw/images"
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

async def scrape_channel(channel_url):
    await client.start()
    channel = await client.get_entity(channel_url)

    today = datetime.now().strftime("%Y-%m-%d")
    channel_name = channel.title.replace(" ", "_")

    channel_dir = os.path.join(RAW_DATA_DIR, today)
    os.makedirs(channel_dir, exist_ok=True)

    output_file = os.path.join(channel_dir, f"{channel_name}.json")
    messages_data = []

    logging.info(f"Scraping channel: {channel_name}")

    async for message in client.iter_messages(channel, limit=200):
        msg = {
            "message_id": message.id,
            "channel_name": channel_name,
            "message_date": message.date.isoformat() if message.date else None,
            "message_text": message.text,
            "views": message.views,
            "forwards": message.forwards,
            "has_media": bool(message.media),
            "image_path": None
        }

        # Download image if present
        if message.photo:
            image_folder = os.path.join(IMAGE_DIR, channel_name)
            os.makedirs(image_folder, exist_ok=True)
            image_path = os.path.join(image_folder, f"{message.id}.jpg")
            await message.download_media(file=image_path)
            msg["image_path"] = image_path

        messages_data.append(msg)

    # Save JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages_data, f, indent=2, ensure_ascii=False)

    logging.info(f"Saved {len(messages_data)} messages to {output_file}")

async def main():
    for channel in CHANNELS:
        try:
            await scrape_channel(channel)
        except Exception as e:
            logging.error(f"Failed scraping {channel}: {e}")

# Run the scraper
with client:
    client.loop.run_until_complete(main())
