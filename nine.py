from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Whatever program")

sc = SparkContext(conf = conf1)

RddExam = sc.textFile("students2.txt")
RddStud = sc.textFile("studentInfo.txt")

def splitResults(x):
	Rec = x.split(",")
	return (Rec[0],Rec[2])

def splitStudent(x):
	Rec = x.split(",")
	return Rec

def add(x,y,):
	return int(x) + int(y)

def scale(x):
	print(x[1])
	if int(x[1]) < 60 * 1.5:
		x[1] = 1
	else:
		x[1] = 0
	print(x[1])
	return x


header1 = RddExam.first()
header2 = RddStud.first()
RddExam1 = RddExam.filter(lambda x: x != header1).map(splitResults)
RddStud1 = RddStud.filter(lambda x: x != header2).map(splitStudent)



RddExam2 = RddExam1.map(scale)

data = RddExam2.collect()

#Data2 = RddStud1.collect()

print(data)
#print("========================")
#print(Data2)
