import json
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from confluent_kafka import Producer

app = FastAPI(title="Seismic Ingestion Gateway")

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER', 'redpanda:9092'),
    'client.id': 'ingestion-gateway'
}
producer = Producer(conf)

class PayloadItem(BaseModel):
    name: str
    values: Optional[Dict[str, Any]] = {}
    accuracy: Optional[int] = 0
    time: Optional[int] = 0 

class AppLogBatch(BaseModel):
    messageId: int
    sessionId: str
    deviceId: str
    payload: List[PayloadItem]

last_known_location = {"lat": None, "lon": None}

def delivery_report(err, msg):
    if err is not None:
        print(f'Errore invio a Redpanda: {err}')

@app.post("/log")
async def ingest_log(batch: AppLogBatch):
    global last_known_location
    try:
        sent_count = 0
        for item in batch.payload:
            if item.name == 'location':
                temp_lat = item.values.get('latitude')
                temp_lon = item.values.get('longitude')
                if temp_lat is not None and temp_lon is not None:
                    last_known_location["lat"] = temp_lat
                    last_known_location["lon"] = temp_lon
                    print(f"🌍 GPS AGGIORNATO IN MEMORIA: {temp_lat}, {temp_lon}")

        for item in batch.payload:
            if item.name in ['totalacceleration', 'accelerometer', 'accelerometeruncalibrated']:
                
                flat_record = {
                    "device_id": batch.deviceId,
                    "session_id": batch.sessionId,
                    "timestamp_ns": item.time,
                    "sensor": item.name,
                    "acc_x": item.values.get('x', 0.0),
                    "acc_y": item.values.get('y', 0.0),
                    "acc_z": item.values.get('z', 0.0),
                    "lat": last_known_location["lat"],
                    "lon": last_known_location["lon"]
                }
                
                producer.produce(
                    'seismic_raw', 
                    key=batch.deviceId, 
                    value=json.dumps(flat_record), 
                    callback=delivery_report
                )
                sent_count += 1
                
        producer.poll(0) 
        
        return {
            "status": "success", 
            "events_processed": len(batch.payload),
            "events_sent_to_broker": sent_count
        }
        
    except Exception as e:
        print(f"Errore di parsing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
def shutdown_event():
    producer.flush()