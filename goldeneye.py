from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Whatever program")

sc = SparkContext(conf = conf1)

Rdd = sc.textFile("user_ratings.txt")

def splitRecord(x):
	Rec=x.split(",")
	return (Rec[1],Rec[2])
def filterGE(x):
	return x

rdd1 = Rdd.map(splitRecord).filter(lambda x: (x[0] == "2"))

Data1 = rdd1.countByValue()

for K in Data1:
	print(K[1],"...", Data1[K])

