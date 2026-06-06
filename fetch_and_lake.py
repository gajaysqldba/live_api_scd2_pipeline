#Impoer the os library to use in our code
import os
# Import the requests libraru to use in the for fetching recoreds from API
import requests
#Import the pandas as pd (giving the alias name) to use in the transformation of data
import pandas as pd
#Import the date and the timezone
from datetime import datetime, timezone
#Import the storage for the GCP storage
from google.cloud import storage
from notifier import send_pipeline_alert



# Configurations - These are the varables for the configuration that we are defining to use the variables for these in the code
API_URL = "https://jsonplaceholder.typicode.com/users"
LOCAL_CSV_PATH = "extracted_users.csv"
BUCKET_NAME = "gajay-customer-pipeline-data"
PROJECT_ID = "bqdemo-496217"

#Function to fetch the api data
def fetch_api_data():
    """Fetch raw JSON data from the open-source API endpoint."""
    print(f"🌐 Connecting to public API: {API_URL}...")
    #Using the method of .get() form requests library to connect and get the data and assigning it to the varailble response
    response = requests.get(API_URL)
    #Using the conditional statement to check if the website is up by verifying the response code 200
    if response.status_code == 200:
        print("✅ Raw JSON successfully downloaded!")
        #If the status code is 200 then returning the response and converting .json format to the python dictonary 
        return response.json()
    #If the status code is not 200 then it will fail to fetch the data
    else:
        print(f"❌ Failed to fetch data. Status Code: {response.status_code}")
        return None

#Defining another function to transform the data
def transform_json_to_csv(json_data):
    """Flatten JSON structural layers and inject ingestion metadata timestamps."""
    print("🧠 Flattening JSON structural layers into columns...")
    #Formatting the dictnory of python to the dataframe in pandas
    df = pd.DataFrame(json_data)
    
    # Extract company name to serve as our subscription tier element
    df['subscription_tier'] = df['company'].apply(lambda x: x['name'] if isinstance(x, dict) else 'Standard')
    
    # Isolate and rename target columns
    #Filtering the reuired columns in the dataframe using double[[]](square brackets) and making a copy using .copy() in the memory of those filtered columns
    df = df[['id', 'name', 'email', 'subscription_tier']].copy()
    #Renaming usning rename method and is doing it inplace
    df.rename(columns={'id': 'customer_id'}, inplace=True)
    
    # Explicitly inject the pipeline timestamp format
    #using the datetime library to ingest the last_updated column for every row
    df['row_last_updated'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    #converting the dataframe to csv and storing it to the local csv path and ingnoring the index(built in feature of pandas)
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
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    try:
        raw_records = fetch_api_data()
        if raw_records:
            transform_json_to_csv(raw_records)
            upload_to_gcs_lake()
            
            # 🟢 SUCCESS EMAIL
            send_pipeline_alert(
                subject=f"🟢 Cloud Build Step 1 SUCCESS: Extraction Run {today}",
                body=f"Phase 1 and 2 completed perfectly.\nRaw API users flattened and streamed to gs://{BUCKET_NAME}/landing_zone/daily_refresh_data.csv"
            )
        else:
            raise Exception("API returned an empty data payload or non-200 response code.")
            
    except Exception as e:
        # 🔴 FAILURE EMAIL
        send_pipeline_alert(
            subject=f"🔴 Cloud Build Step 1 CRITICAL FAILURE: Extraction Run {today}",
            body=f"The pipeline crashed during the extraction/lake landing sequence.\n\nError Traceback:\n{e}"
        )
        # CRITICAL: Re-raise the exception so Cloud Build knows Step 1 failed and stops execution!
        raise
