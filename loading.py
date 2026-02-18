import boto3
import pandas as pd
import io
import config  # Importing your hardcoded settings
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

def setup_db():
    #Checks if the database exists and creates it if it doesn't.
    engine = create_engine(config.DB_URL)
    if not database_exists(engine.url):
        print(f"Creating database: {config.DB_NAME}...")
        create_database(engine.url)
        print("✅ Database created.")
    else:
        print(f"Database '{config.DB_NAME}' already exists.")

def get_latest_processed_file():
    #Finds the newest Parquet file in the 'processed/' folder.
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=config.S3_BUCKET, Prefix="processed/")
    
    all_files = response.get('Contents', [])
    if not all_files:
        return None
    
    # Filter for parquet files and get the latest
    parquet_files = [f for f in all_files if f['Key'].endswith('.parquet')]
    if not parquet_files:
        return None
        
    latest_file = max(parquet_files, key=lambda x: x['LastModified'])
    return latest_file['Key']

def load_to_postgres():
    # 1. Ensure DB is ready
    setup_db()
    
    # 2. Get the file from S3
    latest_key = get_latest_processed_file()
    if not latest_key:
        print("❌ No processed Parquet files found in S3.")
        return

    print(f"📥 Loading data from S3: {latest_key}")
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=config.S3_BUCKET, Key=latest_key)
    
    # 3. Read Parquet binary data into Pandas
    df = pd.read_parquet(io.BytesIO(response['Body'].read()))

    # 4. Push to PostgreSQL
    engine = create_engine(config.DB_URL)
    
    print(f"📤 Writing {len(df)} rows to table 'weather_metrics'...")
    df.to_sql(
        name='weather_metrics', 
        con=engine, 
        if_exists='replace', # Overwrites the table with fresh data
        index=False
    )
    print("🚀 Success! Data is now in your local database.")

if __name__ == "__main__":
    try:
        load_to_postgres()
    except Exception as e:
        print(f"❌ Loading Error: {e}")