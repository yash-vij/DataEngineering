from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NewJSONData").getOrCreate()
d = [(1,"A",10),
        (2,"B",20),
        (3,"C",30),
        (4,"D",40),
        (5,"E",50)]
col = ["ID","Name","Marks"]
sub = [(1,"Hindi"),(2,"English"),(3,"Maths"),(4,"Science")]
col2 = ["ID","Subject"]
df = spark.createDataFrame(data = d,schema=col)
df2 = spark.createDataFrame(data=sub,schema=col2)
df_join = df.join(df2, df.ID == df2.ID, "left").select(df.ID,df.Name,df2.Subject,df.Marks)
df_join.createOrReplaceTempView("df_join")
new_data = spark.sql("select * from df_join where Subject != 'NULL' ")
print(new_data.show())
spark.stop()