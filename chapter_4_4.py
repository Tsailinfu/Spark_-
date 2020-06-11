from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

rdd = sc.parallelize({("pandas", 0), ("pink", 3), ("pirate", 3), ("pandas", 1), ("pink", 4)})
print("rdd:", rdd.collect())
result_1 = rdd.mapValues(lambda x: (x, 1))
print("result_1:", result_1.collect())
result_2 = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y:(x[0] + y[0], x[1] +y[1]))
print("result_2:", result_2.collect())
