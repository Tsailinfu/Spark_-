from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

lines = sc.textFile("D:\\Spark_學習手冊\\data\\README.md")

pythonLines = lines.filter(lambda line: "Python" in line)
print("---------- finish ----------")
