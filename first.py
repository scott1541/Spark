from pyspark import SparkConf, SparkContext

conf1 = SparkConf()

sc = SparkContext(conf = conf1)

List1 = [1,2,3,4,5,6,7,8,9,10]

def double(num):
	return num * 2

Rdd = sc.parallelize(List1)
Data1 = Rdd.collect()
print(Data1)

Rdd2 = Rdd.map(double)

Data2 = Rdd2.collect()
print(Data2)

