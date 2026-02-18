import requests
import json
import boto3
from datetime import datetime
import config  

def run_ingestion():
    # 1. Setup API Details
    API_URL = "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&hourly=temperature_2m"

    # 2. Fetch the data
    print("Fetching data from Open-Meteo...")
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()
        print("✅ Successfully fetched API data.")
        
        # 3. Setup S3 Details
        s3 = boto3.client('s3')
        # Use config.S3_BUCKET instead of hardcoding the string
        BUCKET_NAME = config.S3_BUCKET 
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_name = f"raw/weather_data_{timestamp}.json"
        
        # 4. Upload raw JSON directly to S3
        print(f"Uploading to S3 bucket: {BUCKET_NAME}...")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        print(f"✅ Done! Data saved as: {file_name}")
    else:
        print(f"❌ Failed to fetch data. Status code: {response.status_code}")

if __name__ == "__main__":
    try:
        run_ingestion()
    except Exception as e:
        print(f"❌ Transformation Error: {e}")