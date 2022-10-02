from pyspark.sql.functions import when, col, avg, sum, min, max, row_number
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import lag



spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

df = spark.read.options(header=True, inferschema=True, delimiter=",") \
  .csv("Input/clp dummy.csv")

condition = (F.col('sensor_type') == 'humidity') & (F.col('reading') > 100) | (F.col('reading') <0 )

data = df.filter(~condition)

data.withColumn("New_Temp", when((df.sensor_type == "temperature"),
                df.reading/100).otherwise("NA")).show()

pivoted = df.groupby(df.sensor_id, F.monotonically_increasing_id().alias('index')).pivot("sensor_type").agg(F.first('reading').alias('reading')).drop('index')
pivoted.show()


def dew_point(temperature, humidity):
      dp = temperature-((100- humidity)/5)
      return dp

spark.udf.register("dp", dew_point)


data_frame = pivoted.withColumn("Dew_Point", dew_point(col("temperature"),col("humidity")))
data_frame.write.option("header", True).csv("output")
