from dagster import job, op

@op
def scrape_telegram_data():
    print("Scraping Telegram data...")
    return True

@op
def load_raw_to_postgres(scrape_result):
    print("Loading raw data into Postgres...")
    return True

@op
def run_dbt_transformations(load_result):
    print("Running dbt transformations...")
    return True

@op
def run_yolo_enrichment(dbt_result):
    print("Running YOLO enrichment...")
    return True

@job
def medical_pipeline():
    step1 = scrape_telegram_data()
    step2 = load_raw_to_postgres(step1)
    step3 = run_dbt_transformations(step2)
    run_yolo_enrichment(step3)
