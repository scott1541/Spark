from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Second program")

sc = SparkContext(conf = conf1)

Rdd = sc.textFile("users_short.txt")

def records(x):
	recinfo = x.split("|")
	if recinfo[2] == "M":
		recinfo[2] = "Male"
	else: 
		recinfo[2] = "Female"
	return recinfo

def checkJob(x):
	if (x[3] == "technician") or (x[3] == "administrator"):
		return True
	else:
		return False

Rdd1 = Rdd.map(records)
Rdd2 = Rdd1.filter(checkJob)

Data = Rdd2.collect()

print("")
for record in Data:
	print("Record: " + record[0] + " " + record[1] +" " + record[2] + " " + record[3] + " " + record[4])

Rdd3 = Rdd2.filter(lambda (x,y,z,w,v): int(y) >= 30)
Data1 = Rdd3.collect()

print("")
for record in Data1:
	print("Record: " + record[0] + " " + record[1] +" " + record[2] + " " + record[3] + " " + record[4])