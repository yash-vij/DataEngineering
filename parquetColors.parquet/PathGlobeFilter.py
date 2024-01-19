from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PathGlodeFilter").getOrCreate()
df = spark.read.load("C:/Users/yashv/Spark Programs/Spark/spark-3.5.0-bin-hadoop3/examples/src/main/resources/dir1",
                     format="json",pathGlodeFilter="*.json")
df.show()
spark.stop()