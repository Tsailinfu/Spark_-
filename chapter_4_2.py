from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)


# --------------------  reduceByKey(func) -------------------- #
rdd = sc.parallelize({(1, 2), (3, 4), (3, 6)})
print("rdd:", rdd.collect())
result_1 = rdd.reduceByKey(lambda x, y: x+y)
print("result_1:", result_1.collect())
# --------------------  groupByKey() -------------------- #
result_2 = rdd.groupByKey()
print("result_2:", result_2.collect())
for t in result_2.collect():
    print(t[0], [v for v in t[1]])
# --------------------  combineByKey() -------------------- #
result_3 = rdd.combineByKey((lambda x: (x, 1)),
                            (lambda x, y: (x[0] + y, x[1] + 1)),
                            (lambda x, y: (x[0] + y[0], x[1] + y[1])))
print("result_3", result_3.collect())
# --------------------  mapValues(func) -------------------- #
result_4 = rdd.mapValues(lambda x: x+1)
print("result_4", result_4.collect())
# --------------------  flatMapValues(func) -------------------- #
result_5 = rdd.flatMapValues(lambda x: range(x, 6))
print("result_5", result_5.collect())
# --------------------  keys() -------------------- #
result_6 = rdd.keys()
print("result_6", result_6.collect())
# --------------------  values() -------------------- #
result_7 = rdd.values()
print("result_7", result_7.collect())
# --------------------  sortByKey() -------------------- #
result_8 = rdd.sortByKey()
print("result_8", result_8.collect())
