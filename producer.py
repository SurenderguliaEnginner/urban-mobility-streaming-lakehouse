import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# 1. Kafka Producer Setup
# Hum localhost:9092 se connect kar rahe hain aur data ko JSON format mein serialize kar rahe hain
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'ny_taxi_trips'

def generate_taxi_trip():
    """Real NYC Yellow Taxi schema ke according realistic data generate karta hai"""
    now = datetime.now()
    duration_minutes = random.randint(5, 45)
    dropoff_time = now + timedelta(minutes=duration_minutes)
    
    distance = round(random.uniform(0.5, 18.0), 2)
    # Fare calculation: $3.00 base fare + $2.50 per mile approx
    fare = round(3.0 + (distance * 2.5) + random.uniform(1.0, 5.0), 2)
    
    # 70% log credit card se payment karte hain aur tip dete hain
    payment_type = 1 if random.random() < 0.7 else 2
    tip = round(fare * random.uniform(0.10, 0.25), 2) if payment_type == 1 else 0.0
    total = round(fare + tip + 0.50 + 0.80, 2) # Adding tax & surcharge

    trip_data = {
        "vendor_id": random.choice([1, 2]),
        "pickup_datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": dropoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": random.randint(1, 5),
        "trip_distance": distance,
        "pickup_location_id": random.randint(1, 263),   # NYC Zone IDs
        "dropoff_location_id": random.randint(1, 263),
        "payment_type": payment_type,                   # 1 = Card, 2 = Cash
        "fare_amount": fare,
        "tip_amount": tip,
        "total_amount": total
    }
    return trip_data

print(f"🚀 Streaming NYC Taxi data into Kafka topic: '{TOPIC_NAME}'...")
print("Rukne ke liye keyboard par 'Ctrl + C' dabayein.\n")

try:
    trip_id = 1
    while True:
        trip = generate_taxi_trip()
        # Kafka mein message push kar rahe hain
        producer.send(TOPIC_NAME, value=trip)
        
        print(f"🚕 [Trip #{trip_id}] Zone {trip['pickup_location_id']} ➔ Zone {trip['dropoff_location_id']} | "
              f"Dist: {trip['trip_distance']} mi | Total: ${trip['total_amount']}")
        
        trip_id += 1
        time.sleep(1) # Har 1 second mein 1 trip bhejega

except KeyboardInterrupt:
    print("\n🛑 Stream stopped by user.")
finally:
    producer.flush()
    producer.close()
    print("Producer successfully closed.")