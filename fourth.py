from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Fourth program")

sc = SparkContext(conf = conf1)

List1 = [1,2,3,4,5,6,7]
List2 = [6,7,8,9,10]

Rdd1 = sc.parallelize(List1)
Rdd2 = sc.parallelize(List2)

Rdd3 = Rdd1.union(Rdd2)

Rdd4 = Rdd1.intersection(Rdd2)

Rdd5 = Rdd1.subtract(Rdd2)

Data1 = Rdd1.collect()
Data2 = Rdd2.collect()
Data3 = Rdd3.collect()
Data4 = Rdd4.collect()
Data5 = Rdd5.collect()

print(Data1)
print(Data2)
print(Data3)
print(Data4)
print(Data5)

