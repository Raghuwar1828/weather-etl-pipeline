import ingestion
import transformation
import loading
import time

def start_pipeline():
    print("Starting the Weather Data Pipeline...")
    start_time = time.time()

    try:
        # Step 1: Ingest
        ingestion.run_ingestion()
        
        # Step 2: Transform
        transformation.transform_and_stage()
        
        # Step 3: Load
        loading.load_to_postgres()

        duration = round(time.time() - start_time, 2)
        print(f"\n🏆 Pipeline finished successfully in {duration} seconds!")

    except Exception as e:
        print(f"\n💥 Pipeline crashed: {e}")

if __name__ == "__main__":
    start_pipeline()