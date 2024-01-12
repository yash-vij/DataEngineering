from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("UseOfMap").getOrCreate()
data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]
rdd = spark.sparkContext.parallelize(data)
rdd2 = rdd.map(lambda x:(x,1))
for ele in rdd2.collect():
    print(ele)

