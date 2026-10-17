import glob, os, sys
from pyspark.sql import SparkSession

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

def _setup_env():
    jdks = sorted(glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-17*"))
    if jdks:
        os.environ["JAVA_HOME"] = jdks[-1]
        os.environ["PATH"] = os.path.join(jdks[-1], "bin") + os.pathsep + os.environ["PATH"]
    if os.path.isdir(r"C:\hadoop"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def get_spark(app_name, slots=4):
    """slots = number of local processing slots (used in P1-P4 experiments)."""
    _setup_env()
    return (SparkSession.builder.appName(app_name)
            .master(f"local[{slots}]")
            .config("spark.jars.packages", KAFKA_PKG)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", __import__("common.config", fromlist=["x"]).load_config().get("spark_driver_memory", "1g"))            .getOrCreate())