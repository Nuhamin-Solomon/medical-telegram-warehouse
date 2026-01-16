from fastapi import APIRouter
from api.database import SessionLocal
from api.schemas.analytics import Detection
from sqlalchemy import text

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/detections", response_model=list[Detection])
def get_detections():
    db = SessionLocal()
    result = db.execute(text("""
        SELECT message_id, detected_object, confidence
        FROM fct_image_detections
        LIMIT 100
    """))
    rows = result.fetchall()
    db.close()

    return [
        {
            "message_id": r[0],
            "detected_object": r[1],
            "confidence": r[2]
        }
        for r in rows
    ]
