from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Third program")

sc = SparkContext(conf = conf1)

Rdd = sc.textFile("students.txt")

def records(x):
	x = x.replace("Gender,", "")
	x = x.replace("F,", "")
	x = x.replace("M,", "")
	x = x + ",Percentage,Result"
	recinfo = x.split(",")

	return recinfo

Rdd1 = Rdd.map(records)


def process(x):
	if x[0] != "ID":
		x[3] = round(int(x[2]) * 0.6666)
		x[3] = str(x[3]) 
		if int(x[2]) >= 60:
			x[4] = "Pass"
		else:
			x[4] = "Fail"
	return x

Rdd2 = Rdd1.map(process)

Data = Rdd2.collect()

print("")
print("\tID  Name  Mark  Percent  Result")
for record in Data[1:]:
	print("Record: " + record[0] + " " + record[1] +" " + record[2] + " " + record[3] + " " + record[4])