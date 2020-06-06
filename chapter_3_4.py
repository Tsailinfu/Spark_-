from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

#  Python 將一個 RDD 內的數值平方
num = sc.parallelize([1, 2, 3, 4])
squared = num.map(lambda x: x * x).collect()
for num in squared:
    print(num)
