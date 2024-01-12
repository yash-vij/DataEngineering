from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf = conf)
data_ = [1,2,3,4,5,6,7,8,9,0]
par_data = sc.parallelize(data_,10)
print(par_data.reduce(lambda a,b : a+b))
