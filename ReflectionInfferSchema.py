from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName("ReflectionInferSchema").getOrCreate()
sc = spark.sparkContext
lines = sc.textFile("hello.txt")

parts = lines.map(lambda x : x.split(","))

people = parts.map(lambda x : Row(EMP_ID = x[0], Name = x[1]))

df = spark.createDataFrame(people)
df_schema = df.createOrReplaceTempView("PeopleData")

p = spark.sql("select * from PeopleData Where EMP_ID >150")

print(p.show())
