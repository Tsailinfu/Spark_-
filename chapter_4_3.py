from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

rdd = sc.parallelize({(1, 2), (3, 4), (3, 6)})
print("rdd:", rdd.collect())
other = sc.parallelize({(3, 9)})
print("other:", other.collect())
# -------------------- subtractByKey() -------------------- #
result_1 = rdd.subtractByKey(other)
print("result_1", result_1.collect())
# -------------------- join() -------------------- #
result_2 = rdd.join(other)
print("result_2", result_2.collect())
# -------------------- rightOuterJoin() -------------------- #
result_3 = rdd.rightOuterJoin(other)
print("result_3", result_3.collect())
# -------------------- leftOuterJoin() -------------------- #
result_4 = rdd.leftOuterJoin(other)
print("result_4", result_4.collect())
