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

rdd1 = Rdd.map(splitRecord).filter(lambda x: (x[0] == "2") and (x[1] =="5"))
rdd2 = Rdd.map(splitRecord).filter(lambda x: (x[0] == "2") and (x[1] =="4"))
rdd3 = Rdd.map(splitRecord).filter(lambda x: (x[0] == "2") and (x[1] =="3"))
rdd4 = Rdd.map(splitRecord).filter(lambda x: (x[0] == "2") and (x[1] =="2"))
rdd5 = Rdd.map(splitRecord).filter(lambda x: (x[0] == "2") and (x[1] =="1"))

print("5*  ", rdd1.count())
print("4*  ", rdd2.count())
print("3*  ", rdd3.count())
print("2*  ", rdd4.count())
print("1*  ", rdd5.count())
