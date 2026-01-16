import os
import pandas as pd
from ultralytics import YOLO

IMAGE_ROOT = "data/raw/images"
OUTPUT_CSV = "data/processed/yolo_detections.csv"

model = YOLO("yolov8n.pt")
results_data = []

for root, dirs, files in os.walk(IMAGE_ROOT):
    for file in files:
        if file.lower().endswith((".jpg", ".png")):
            image_path = os.path.join(root, file)
            message_id = os.path.splitext(file)[0]

            results = model(image_path)[0]

            for box in results.boxes:
                class_id = int(box.cls[0])
                label = model.names[class_id]
                confidence = float(box.conf[0])

                results_data.append({
                    "message_id": message_id,
                    "image_path": image_path,
                    "detected_object": label,
                    "confidence": confidence
                })

df = pd.DataFrame(results_data)
os.makedirs("data/processed", exist_ok=True)
df.to_csv(OUTPUT_CSV, index=False)

print(f"Saved detection results to {OUTPUT_CSV}")
