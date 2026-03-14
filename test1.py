from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test1").getOrCreate()


