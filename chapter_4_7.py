from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

data = {(1, 2), (3, 4), (3, 6)}
rdd = sc.parallelize(data)
print("rdd:", rdd.collect())
# -------------------- countByKey() -------------------- #
result_1 = rdd.countByKey()
print("result_1", result_1)
# -------------------- collectAsMap() -------------------- #
result_2 = rdd.collectAsMap()
print("result_2", result_2)
# -------------------- lookup(key) -------------------- #
result_3 = rdd.lookup(3)
print("result_3", result_3)
