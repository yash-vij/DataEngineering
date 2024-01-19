from pyspark.sql import SparkSession
import time
if __name__ == "__main__":
    logFile = "README.txt"  # Should be some file on your system
    spark = SparkSession.builder.master("local").appName("SimpleApp").getOrCreate()
    logData = spark.read.text(logFile).cache()

    numAs = logData.filter(logData.value.contains('a')).count()
    numBs = logData.filter(logData.value.contains('b')).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
    time.sleep(100)
    logData.show()
    #spark.stop()