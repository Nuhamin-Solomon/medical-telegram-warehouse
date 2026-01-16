from fastapi import FastAPI
from api.routers.analytics import router as analytics_router

app = FastAPI(title="Medical Analytics API")

app.include_router(analytics_router)

@app.get("/")
def root():
    return {"status": "API running"}
