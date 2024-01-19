from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[2]').appName('Aggregation').getOrCreate()
df = spark.read.json('employee_data.json')
print(df)