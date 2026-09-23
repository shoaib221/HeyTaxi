import os
import requests
from pyspark.sql.functions import count, avg, sum as spark_sum, round as spark_round, col

SILVER_DELTA_PATH = "./data/delta/silver_yellow_taxi"
GOLD_DELTA_PATH = "./data/delta/gold_location_performance_enriched"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
LOCAL_ZONE_CSV = "./data/lookup/taxi_zone_lookup.csv"

def download_zone_lookup():
    """Downloads the NYC taxi zone lookup file if not present locally."""
    os.makedirs("./data/lookup", exist_ok=True)
    if not os.path.exists(LOCAL_ZONE_CSV):
        print(f"Downloading taxi zone lookup CSV to {LOCAL_ZONE_CSV}...")
        res = requests.get(ZONE_LOOKUP_URL)
        res.raise_for_status()
        with open(LOCAL_ZONE_CSV, "wb") as f:
            f.write(res.content)
        print("Zone lookup download complete.")

def run_gold(spark):
    print("\n--- Starting Gold Layer Processing ---")
    
    # 1. Check prerequisites
    if not os.path.exists(SILVER_DELTA_PATH):
        raise FileNotFoundError(f"Silver Delta table not found at {SILVER_DELTA_PATH}. Run 02_silver.py first!")
        
    download_zone_lookup()
    
    # 2. Read Silver Delta table
    df_silver = spark.read.format("delta").load(SILVER_DELTA_PATH)
    
    # 3. Read local Zone lookup CSV
    df_zones = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(LOCAL_ZONE_CSV)
        
    # 4. Join Silver with Zone lookup on PULocationID == LocationID
    df_enriched = df_silver.join(
        df_zones,
        df_silver.PULocationID == df_zones.LocationID,
        how="left"
    )
    
    # 5. Aggregate metrics by Borough, Zone (Neighborhood), and Location ID
    df_gold = df_enriched.groupBy("Borough", "Zone", "PULocationID") \
        .agg(
            count("*").alias("total_trips"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue")
        )
        
    # 6. Write to local Gold Delta Table
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_DELTA_PATH)
        
    gold_count = spark.read.format("delta").load(GOLD_DELTA_PATH).count()
    print(f"Gold Layer complete!")
    print(f" Aggregated Zone Groups: {gold_count:,}")
    print(f" Saved to               : {GOLD_DELTA_PATH}")

if __name__ == "__main__":
    from src.utils.spark_session import get_spark_session
    spark_sess = get_spark_session("Local_Gold_Test")
    run_gold(spark_sess)
    