from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Sixth program")

sc = SparkContext(conf = conf1)

Rdd = sc.textFile("students.txt")

def records(x):
	x = x.replace("Gender,", "")
	x = x.replace("F,", "")
	x = x.replace("M,", "")
	x = x + ",MarkW,Percentage,Result"
	print(x)
	recinfo = x.split(",")

	return recinfo

Rdd1 = Rdd.map(records)

def intToWord(x):
	num = int(x)

	word = ""

	onesD = {0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 
				11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen", 15: "fifteen", 16: "sixteen", 17: "seventeen", 18: "eighteen", 19: "nineteen"}

	tensD = {2: "twenty",3: "thirty", 4: "fourty", 5: "fifty", 6: "sixty", 7: "seventy", 8: "eighty",9: "ninety", 0:"zero"}

	if (num > 0) and (num <= 19) :
		return onesD[num]
	elif (num >= 20) and (num <= 99):
		tens, ones = divmod(num, 10)
		return tensD[tens - 2] + " " + onesD[ones]
	elif num > 99:
		return "one hundred" + intToWord(num-100)
	else:
		return

def process(x):
	if x[0] != "ID":
		x[4] = round(int(x[2]) * 0.6666)
		x[4] = str(x[4]) 
		if int(x[2]) >= 60:
			x[5] = "Pass"
		else:
			x[5] = "Fail"

		x[3] = str(intToWord(x[2]))
	return x

Rdd2 = Rdd1.map(process)

Data = Rdd2.collect()

print("")
print("\tID  Name  Mark  Percent  Result")
for record in Data[1:]:
	#print(record)
	print("Record: " + record[0] + " " + record[1] +" " + record[2] + " " + record[3] + " " + record[4] + " " + record[5])