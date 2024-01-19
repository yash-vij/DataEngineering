from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ParquetData").getOrCreate()
#df = spark.read.json("C:/Users/yashv/Spark Programs/Spark/spark-3.5.0-bin-hadoop3/examples/src/main/resources/people.json")
#df.write.parquet("C:/Users/yashv/Spark Programs/Spark/spark-3.5.0-bin-hadoop3/examples/src/main/resources/people_new.parquet")

df2 = spark.read.parquet("C:/Users/yashv/Spark Programs/Spark/spark-3.5.0-bin-hadoop3/examples/src/main/resources/people_new.parquet")
df2.createOrReplaceTempView("ParquetData")
print(spark.sql("SELECT * from ParquetData").show())