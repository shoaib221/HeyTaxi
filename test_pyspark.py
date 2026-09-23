from src.utils.spark_session import get_spark_session

try:
    spark = get_spark_session("Java_Sanity_Test")
    # Quick execution test
    df = spark.createDataFrame([(1, "Java OK")], ["id", "status"])
    df.show()
    print("\n✅ Success: PySpark connected to Java successfully!")
    spark.stop()
except Exception as e:
    print("\n❌ Failure: PySpark could not connect to Java.")
    print(e)