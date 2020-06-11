from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

rdd = sc.textFile("D:\\Spark_學習手冊\\data\\TakeMeHomeCountryRoads.txt")
words = rdd.flatMap(lambda x:x.split(" "))
result = words.map(lambda x:(x, 1)).reduceByKey(lambda x, y:x + y)
print("result", result.collect())
