from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Basics").getOrCreate()
df = spark.read.option("header",True).csv("Employee Sample Data.csv")
df.createOrReplaceTempView("Employee")
print(spark.sql(" select Gender, sum(`Annual Salary`) as Total_Annual_Salary from Employee group by Gender").show())
spark.stop()