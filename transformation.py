import boto3
import json
import pandas as pd
import config  # This looks for config.py in the same folder
import io
from datetime import datetime

def get_latest_raw_file():
    s3 = boto3.client('s3')
    # Use config.S3_BUCKET directly
    response = s3.list_objects_v2(Bucket=config.S3_BUCKET, Prefix="raw/")
    
    all_files = response.get('Contents', [])
    if not all_files:
        return None
    
    # Finds the newest file by timestamp
    latest_file = max(all_files, key=lambda x: x['LastModified'])
    return latest_file['Key']

def transform_and_stage():
    # --- DEBUG CHECK ---
    print(f"Checking bucket: {config.S3_BUCKET}")
    
    latest_raw = get_latest_raw_file()
    if not latest_raw:
        print("❌ No files found in the 'raw/' folder.")
        return

    print(f"Reading latest raw file: {latest_raw}")
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=config.S3_BUCKET, Key=latest_raw)
    data = json.loads(response['Body'].read().decode('utf-8'))

    # 1. Transform JSON to DataFrame
    df = pd.DataFrame(data['hourly'])
    df['time'] = pd.to_datetime(df['time'])
    
    # 2. Add our calculations
    df.rename(columns={'temperature_2m': 'temp_celsius'}, inplace=True)
    df['temp_fahrenheit'] = (df['temp_celsius'] * 9/5) + 32

    # 3. Convert DataFrame to Parquet format in memory
    processed_key = f"processed/cleaned_weather_{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    
    # 4. Upload to S3
    s3.put_object(
        Bucket=config.S3_BUCKET, 
        Key=processed_key, 
        Body=parquet_buffer.getvalue()
    )
    
    print(f"✅ Successfully saved parquet to: {processed_key}")
    print(df.head())

if __name__ == "__main__":
    try:
        transform_and_stage()
    except Exception as e:
        print(f"❌ Transformation Error: {e}")