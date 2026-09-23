import os
import requests
from pyspark.sql.functions import current_timestamp, lit

RAW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
LOCAL_RAW_PATH = "./data/raw/yellow_tripdata_2024-01.parquet"

def run_bronze(spark):
    os.makedirs("./data/raw", exist_ok=True)
    
    # 1. Download raw file locally
    if not os.path.exists(LOCAL_RAW_PATH):
        print("Downloading raw Parquet file...")
        res = requests.get(RAW_URL)
        with open(LOCAL_RAW_PATH, "wb") as f:
            f.write(res.content)
            
    # 2. Read into Spark
    df_raw = spark.read.parquet(LOCAL_RAW_PATH)
    
    # 3. Add audit columns (using standard file path string locally)
    df_bronze = df_raw \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("source_file", lit(LOCAL_RAW_PATH))
        
    # 4. Save as local Delta Table
    bronze_table_path = "./data/delta/bronze_yellow_taxi"
    df_bronze.write.format("delta").mode("overwrite").save(bronze_table_path)
    print(f"Bronze Layer complete! Saved to {bronze_table_path}")