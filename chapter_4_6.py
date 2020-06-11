from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

data = [("a", 3), ("b", 4), ("a", 1)]
result_1 = sc.parallelize(data).reduceByKey(lambda x, y: x + y)
print("result_1\n", result_1.collect())
result_2 = sc.parallelize(data).reduceByKey(lambda x, y: x + y, 10)
print("result_2\n", result_2.collect())
