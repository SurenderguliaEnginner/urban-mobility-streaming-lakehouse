import json
from datetime import datetime
from kafka import KafkaConsumer
from google.cloud import storage

# 1. Configuration
TOPIC_NAME = 'ny_taxi_trips'
BUCKET_NAME = 'shriom-ny-taxi-lake-2026'
KEY_PATH = 'key.json'
BATCH_SIZE = 50  # Har 50 trips ke baad ek file upload hogi

# 2. Google Cloud Storage Client Setup
print("Connecting to Google Cloud Storage...")
storage_client = storage.Client.from_service_account_json(KEY_PATH)
bucket = storage_client.bucket(BUCKET_NAME)

# 3. Kafka Consumer Setup
print("Connecting to Kafka Topic...")
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',       # Purane chhoot gaye messages bhi read karega
    enable_auto_commit=True,
    group_id='taxi-gcs-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def upload_batch_to_gcs(records, batch_id):
    """50 records ko JSON Lines format mein GCS Bucket par upload karta hai"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"raw/taxi_trips_{timestamp}_batch{batch_id}.json"
    blob = bucket.blob(file_name)
    
    # Databricks aur BigQuery ke liye sabse best format: newline-delimited JSON
    file_content = "\n".join([json.dumps(record) for record in records])
    blob.upload_from_string(file_content, content_type='application/json')
    
    print(f"☁️ [Uploaded to GCS] ➔ gs://{BUCKET_NAME}/{file_name} ({len(records)} trips)")

print(f"📥 Consumer started! Listening to '{TOPIC_NAME}'...")
print(f"Data will be uploaded to: gs://{BUCKET_NAME}/raw/ every {BATCH_SIZE} trips.\n")

batch = []
batch_count = 1

try:
    for message in consumer:
        trip = message.value
        batch.append(trip)
        
        # Terminal pe progress dekhne ke liye
        print(f"📦 Collected: {len(batch)}/{BATCH_SIZE} trips...", end="\r")
        
        if len(batch) >= BATCH_SIZE:
            print() # Newline
            upload_batch_to_gcs(batch, batch_count)
            batch = []
            batch_count += 1

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
    if batch:
        print(f"Uploading remaining {len(batch)} records before exit...")
        upload_batch_to_gcs(batch, batch_count)
finally:
    consumer.close()
    print("Consumer closed successfully.")