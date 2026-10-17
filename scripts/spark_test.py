from common.config import load_config
from common.spark_utils import get_spark

cfg = load_config()
spark = get_spark("spark-test", 2)
spark.sparkContext.setLogLevel("WARN")

df = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", cfg["bootstrap"])
      .option("subscribe", cfg["topics"]["raw"])
      .option("startingOffsets", "earliest")
      .load())

print("rows in taxi.raw:", df.count())
df.groupBy("partition").count().orderBy("partition").show()
spark.stop()