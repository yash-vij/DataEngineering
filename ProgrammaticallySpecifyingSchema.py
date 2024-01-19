from pyspark.sql.types import StringType,StructField,StructType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

file = sc.textFile("hello.txt")
parts = file.map(lambda x : x.split(","))
people = parts.map(lambda x  : (x[0],x[1].strip()))
schemaString = "EMP_ID Name"
schemaPeople = [StructField(f,StringType(),True) for f in schemaString.split() ]
schema = StructType(schemaPeople)
df = spark.createDataFrame(people,schema)
print(df.show())
