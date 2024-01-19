from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("IgnoreCorruptFile").getOrCreate()
spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
test_corrupt = spark.read.parquet("C:/Users/yashv/Spark Programs/Spark/spark-3.5.0-bin-hadoop3/examples/src/main/resources/dir1",
                                  "C:/Users/yashv/Spark Programs/Spark/spark-3.5.0-bin-hadoop3/examples/src/main/resources/dir1/dir2")
test_corrupt.show()
