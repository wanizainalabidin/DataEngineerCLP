from pyspark.sql.functions import when, col, avg, sum, min, max, row_number
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import lag




spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

#Read data from a csv file using pyspark. The dummy file contains variables provided by CLP but also, additional rows to check if data meets the business requirements below

df = spark.read.options(header=True, inferschema=True, delimiter=",") \
  .csv("Input/clp dummy.csv")


#Answer business requirement 1: Remove rows when the humidity is more 100 or when it's negative
condition = (F.col('sensor_type') == 'humidity') & (F.col('reading') > 100) | (F.col('reading') <0 )
data = df.filter(~condition)


#Answer business requirement 2: For temperature column, scale it down by dividing by 100. 
data.withColumn("New_Temp", when((df.sensor_type == "temperature"),
                df.reading/100).otherwise("NA")).show()


#Answer business requirement 3: Pivoting the data so that Humidity and Temperature will be in column format rather than rows.
pivoted = df.groupby(df.sensor_id, F.monotonically_increasing_id().alias('index')).pivot("sensor_type").agg(F.first('reading').alias('reading')).drop('index')
pivoted.show()


# Define a UDF to calculate the Dew Point. Currently, even when we pivot the columns, each sensor only either has temperature or humidity and not both. As such,
#the calculation for dew point returns null values.
def dew_point(temperature, humidity):
      dp = temperature-((100- humidity)/5)
      return dp

spark.udf.register("dp", dew_point)

#Convert it into a dataframe and save the file as csv. 
data_frame = pivoted.withColumn("Dew_Point", dew_point(col("temperature"),col("humidity")))
data_frame.write.option("header", True).csv("output")
