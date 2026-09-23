import os
from pyspark.sql.functions import col, unix_timestamp, round as spark_round

BRONZE_DELTA_PATH = "./data/delta/bronze_yellow_taxi"
SILVER_DELTA_PATH = "./data/delta/silver_yellow_taxi"

def run_silver(spark):
    print("\n--- Starting Silver Layer Processing ---")
    
    # 1. Verify Bronze table exists
    if not os.path.exists(BRONZE_DELTA_PATH):
        raise FileNotFoundError(f"Bronze Delta table not found at {BRONZE_DELTA_PATH}. Run 01_bronze.py first!")
        
    # 2. Read from local Bronze Delta table
    df_bronze = spark.read.format("delta").load(BRONZE_DELTA_PATH)
    initial_count = df_bronze.count()
    print(f"Loaded Bronze records: {initial_count:,}")
    
    # 3. Apply cleaning, date boundaries, and feature engineering
    df_silver = df_bronze \
        .filter(
            # Standardize date range to Jan 2024
            (col("tpep_pickup_datetime") >= "2024-01-01") & 
            (col("tpep_pickup_datetime") < "2024-02-01") &
            # Business logic filters
            (col("passenger_count") > 0) &
            (col("trip_distance") > 0.0) &
            (col("fare_amount") > 0.0)
        ) \
        .withColumn(
            "trip_duration_minutes", 
            spark_round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
        ) \
        .filter(
            # Keep realistic trip durations (between 1 minute and 24 hours)
            (col("trip_duration_minutes") >= 1.0) & 
            (col("trip_duration_minutes") <= 1440.0)
        )
        
    # 4. Write to local Silver Delta table
    os.makedirs("./data/delta", exist_ok=True)
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(SILVER_DELTA_PATH)
        
    clean_count = spark.read.format("delta").load(SILVER_DELTA_PATH).count()
    dropped_count = initial_count - clean_count
    
    print(f"Silver Layer complete!")
    print(f" Clean Records Saved : {clean_count:,}")
    print(f" Outlier Rows Dropped : {dropped_count:,}")
    print(f" Saved to             : {SILVER_DELTA_PATH}")

if __name__ == "__main__":
    from src.utils.spark_session import get_spark_session
    spark_sess = get_spark_session("Local_Silver_Test")
    run_silver(spark_sess)