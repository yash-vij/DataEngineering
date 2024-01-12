from pyspark import SparkContext
def myFunc(s):
    words = s.split(",")
    return (words,len(words))
sc = SparkContext()
d = sc.textFile("hello.txt").map(myFunc)
for el in d.collect():
    print(el)

sc.stop()
