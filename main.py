from src.utils.spark_session import get_spark_session
from src.pipeline.bronze import run_bronze
from src.pipeline.silver import run_silver
from src.pipeline.gold import run_gold

if __name__ == "__main__":
    print("Starting Local Medallion Pipeline...")
    spark = get_spark_session()
    
    run_bronze(spark)
    # run_silver(spark)
    # run_gold(spark)
    
    print("Pipeline Execution Finished Successfully!")
    spark.stop()