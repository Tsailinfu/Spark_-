from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

#  python 的 parallelize() 方法
lines = sc.parallelize(["pandas", "i like pandas"])
print("lines\n", lines.collect())
