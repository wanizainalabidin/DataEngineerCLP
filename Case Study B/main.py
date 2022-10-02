from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("readfromjson").master("local[4]").getOrCreate()

multiline_df = spark.read.option("multiline","true") \
      .json("clp.json")


# Create Schema of the JSON column

schema = StructType([
    StructField("device_id",IntegerType(),True),
    StructField("device_name",StringType(),True),
    StructField("time",TimestampType(),True),
StructField("delta_energy",FloatType(),True)
  ])


explode = multiline_df.withColumn("object", F.explode("measurements"))
explode.show(truncate=False)

explode = explode.select(
                                "object.cumulative_energy",
                                "object.device_id",
                                "object.device_name",
"object.time")


windowSpec = Window.partitionBy("device_id").orderBy("time")
dataset = explode.select("*", (F.col("cumulative_energy") - F.lag(F.col("cumulative_energy"),1,0).over(windowSpec))\
                          .alias('delta_energy'))


dataset = dataset.select(col("device_id"),col("device_name"), col("time"), col("delta_energy"))

dataset.withColumn("device_id",col("device_id").cast(IntegerType()))
dataset.withColumn("device_name",col("device_name").cast(StringType()))
dataset.withColumn("time",col("time").cast(TimestampType())).show()
dataset.withColumn("delta_energy",col("delta_energy").cast(FloatType()))

dataset.write.parquet("clp.parquet")

