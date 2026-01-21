# medical_warehouse/dagster_pipeline.py
from dagster import job, op, schedule

@op
def scrape_telegram():
    print("Scraping Telegram...")

@op
def load_to_db():
    print("Loading data...")

@op
def run_dbt():
    print("Running dbt transformations...")

@job
def telegram_pipeline():
    scrape_telegram()
    load_to_db()
    run_dbt()

@schedule(cron_schedule="0 0 * * *", job=telegram_pipeline)
def daily_schedule(_context):
    return {}
