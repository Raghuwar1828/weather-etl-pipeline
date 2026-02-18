# AWS S3 Settings
S3_BUCKET = "weather-api-data-raw-raghus"

# PostgreSQL Connection Details
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "weather_db"

# This builds the URL that SQLAlchemy needs
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"