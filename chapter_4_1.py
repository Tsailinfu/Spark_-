from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

# Python 的 textFile() 方法
lines = sc.textFile("D:\\Spark_學習手冊\\data\\README.md")

print("lines\n", lines.collect())
pairs = lines.map(lambda x: (x.split(" ")[0], x))
print("pairs", pairs.collect())
