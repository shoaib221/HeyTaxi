import os
import sys

# Ensure project root is in python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.utils.spark_session import get_spark_session

BRONZE_DELTA_PATH = "./data/delta/bronze_yellow_taxi"

def inspect_bronze():
    spark = get_spark_session("Inspect_Bronze_Table")
    
    # 1. Read the local Delta table
    print(f"Loading Bronze Delta table from: {BRONZE_DELTA_PATH}\n")
    df_bronze = spark.read.format("delta").load(BRONZE_DELTA_PATH)
    
    # 2. Print Total Row Count
    total_rows = df_bronze.count()
    print("=" * 50)
    print(f"TOTAL ROW COUNT: {total_rows:,}")
    print("=" * 50)
    
    # 3. Print Schema
    print("\n--- BRONZE TABLE SCHEMA ---")
    df_bronze.printSchema()
    
    # 4. Display Sample Rows (showing key columns & audit metadata)
    print("\n--- SAMPLE DATA (FIRST 5 ROWS) ---")
    df_bronze.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "ingestion_time",  # Audit metadata column
        "source_file"      # Audit metadata column
    ).show(5, truncate=False)

    print("\n--- INSPECT BRONZE COMPLETED ---")
    spark.stop()



if __name__ == "__main__":
    inspect_bronze()