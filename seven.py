from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Sixth program")

sc = SparkContext(conf = conf1)

List1 = [1,2,3,4,5,5,5,3]
Rdd = sc.parallelize(List1)

Data1 = Rdd.countByValue()

for K in Data1:
	print(K,"...", Data1[K])
print("======================")
for K in Data1:
	print(K,"...", 3)

def add(x,y):
	return x + y
def maxi(x,y):
	if x > y:
		return x
	else:
		return y
def mini(x,y):
	if x < y:
		return x
	else:
		return y

Rdd1 = sc.parallelize([7,2,3,4,5,23,4,1,8,3,13])

Data2 = Rdd1.reduce(add)
Data3 = Rdd1.reduce(maxi)
Data4 = Rdd1.reduce(mini)

print(Data2)
print(Data3)
print(Data4)