from pydantic import BaseModel

class Detection(BaseModel):
    message_id: int
    detected_object: str
    confidence: float
