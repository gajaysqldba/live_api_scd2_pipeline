import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage

# Configurations
API_URL = "https://jsonplaceholder.typicode.com/users"
LOCAL_CSV_PATH = "extracted_users.csv"
BUCKET_NAME = "gajay-customer-pipeline-data"
PROJECT_ID = "bqdemo-496217"

def fetch_api_data():
    """Fetch raw JSON data from the open-source API endpoint."""
    print(f"🌐 Connecting to public API: {API_URL}...")
    response = requests.get(API_URL)
    if response.status_code == 200:
        print("✅ Raw JSON successfully downloaded!")
        return response.json()
    else:
        print(f"❌ Failed to fetch data. Status Code: {response.status_code}")
        return None

def transform_json_to_csv(json_data):
    """Flatten JSON structural layers and inject ingestion metadata timestamps."""
    print("🧠 Flattening JSON structural layers into columns...")
    df = pd.DataFrame(json_data)
    
    # Extract company name to serve as our subscription tier element
    df['subscription_tier'] = df['company'].apply(lambda x: x['name'] if isinstance(x, dict) else 'Standard')
    
    # Isolate and rename target columns
    df = df[['id', 'name', 'email', 'subscription_tier']].copy()
    df.rename(columns={'id': 'customer_id'}, inplace=True)
    
    # Explicitly inject the pipeline timestamp format
    df['row_last_updated'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    df.to_csv(LOCAL_CSV_PATH, index=False)
    print(f"💾 Cleaned data saved locally to: {LOCAL_CSV_PATH}")

def upload_to_gcs_lake():
    """Ship the file straight into our Cloud Storage Data Lake using runtime credentials."""
    print(f"📦 Connecting to Cloud Storage bucket: {BUCKET_NAME}...")
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob("landing_zone/daily_refresh_data.csv")
    
    print(f"📤 Uploading local CSV -> gs://{BUCKET_NAME}/landing_zone/daily_refresh_data.csv...")
    blob.upload_from_filename(LOCAL_CSV_PATH)
    print("🚀 Data lake landing zone successfully updated!")
    
    if os.path.exists(LOCAL_CSV_PATH):
        os.remove(LOCAL_CSV_PATH)

if __name__ == "__main__":
    raw_records = fetch_api_data()
    if raw_records:
        transform_json_to_csv(raw_records)
        upload_to_gcs_lake()