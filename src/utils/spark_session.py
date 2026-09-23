import os
import sys

# 1. Point directly to your JDK 20 installation folder
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-20"

# 2. Point to your local Hadoop binaries folder
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# 3. Add Java bin and Hadoop bin directly to the front of PATH
java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
hadoop_bin = r"C:\hadoop\bin"

os.environ["PATH"] = java_bin + os.pathsep + hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session(app_name: str = "NYC_Taxi_Local_Pipeline") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "./data/delta")
        .config("spark.driver.memory", "4g")
        .master("local[*]")
    )
    
    return configure_spark_with_delta_pip(builder).getOrCreate()