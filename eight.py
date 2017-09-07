from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Whatever program")

sc = SparkContext(conf = conf1)

Rdd = sc.textFile("students2.txt")

def splitRecord(X):
	Rec=X.split(",")
	return (Rec[0],Rec[2])

def add(a,b):
	return int(a)+int(b)

header = Rdd.first()
Rdd1 = Rdd.filter(lambda x: x != header)

Rdd2 = Rdd1.map(splitRecord)
Rdd3=Rdd2.reduceByKey(add)
data=Rdd3.collect()

print (data)

for key,value in data:
	per=value*100/450
	if per>=60:
		print (key,"....",value,"--- Pass")
	else:
		print (key,"....",value,"--- Fail")



