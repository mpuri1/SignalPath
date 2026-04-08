import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_telemetry(train_id):
    """
    Simulate train telemetry: JSON event with speed, location, and sensor data.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "train_id": train_id,
        "timestamp": datetime.utcnow().isoformat(),
        "speed_mph": round(random.uniform(30.0, 75.0), 2),
        "latitude": round(random.uniform(30.0, 48.0), 6),
        "longitude": round(random.uniform(-125.0, -70.0), 6),
        "bearing_temp_f": round(random.uniform(90.0, 160.0), 2),
        "fuel_level_pct": round(random.uniform(20.0, 100.0), 2),
        "status": random.choice(["MOVING", "STATIONARY", "MAINTENANCE"])
    }

def main():
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET'),
    }

    # Fallback to local printing if no credentials
    is_mock = not all([conf['bootstrap.servers'], conf['sasl.username'], conf['sasl.password']])
    
    if is_mock:
        print("⚠️  No Kafka credentials found. Running in MOCK mode (local printing only).")
        producer = None
    else:
        print(f"🚀 Initializing Kafka Producer for {conf['bootstrap.servers']}")
        producer = Producer(conf)

    trains = ["EXPRESS-101", "EXPRESS-202", "FREIGHT-303", "FREIGHT-909"]
    topic = "rail_telemetry"

    try:
        while True:
            for train in trains:
                telemetry = generate_telemetry(train)
                payload = json.dumps(telemetry).encode('utf-8')
                
                if producer:
                    producer.produce(topic, key=train, value=payload, callback=delivery_report)
                    producer.poll(0)
                else:
                    print(f"DEBUG: {telemetry}")
                
            time.sleep(2)  # Emit every 2 seconds
            if producer:
                producer.flush()
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        if producer:
            producer.flush()

if __name__ == "__main__":
    main()
