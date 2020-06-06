from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

# Python 的 flatMap(), 切個字串為單字
lines = sc.parallelize(["Hello world", "hi"])
words = lines.flatMap(lambda line: line.split(" "))
print("words\n", words.collect())
print("words first\n", words.first())
