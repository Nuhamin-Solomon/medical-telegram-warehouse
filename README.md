# medical-telegram-warehouse
End-to-end ELT pipeline for scraping Telegram medical data, transforming it into a data warehouse, enriching with YOLO, and serving analytics via FastAPI.
Project Overview
This project:

Scrapes public Telegram medical channels
Stores raw data in a data lake
Transforms data using dbt into a star schema
Enriches image data using YOLOv8
Exposes analytics via FastAPI
Orchestrates pipelines using Dagster
Tech Stack
Python
Telethon
PostgreSQL
dbt
YOLOv8
FastAPI
Dagster
Docker
Project Structure
medical-telegram-warehouse/ ├── data/ ├── src/ ├── medical_warehouse/ ├── api/ ├── logs/ ├── tests/ ├── docker-compose.yml ├── Dockerfile └── README.md
