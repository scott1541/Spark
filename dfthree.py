from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

conf1 = SparkConf()

sc = SparkContext(conf = conf1)
sql = SQLContext(sc)

def split(x):
	record = x.split(",")
	return int(record[0]), record[1], record[2], record[3], int(record[4])

def grade(x):
	if int(x) >= 90:
		return "A+"
	elif int(x) >= 80:
		return "A"
	elif int(x) >= 70:
		return "B"
	elif int(x) >= 60:
		return "C"
	else:
		return "Fail"

#sql.udf.register("grd", grade)
grd = udf(grade, StringType())

Rdd1 = sc.textFile("file:///home/cloudera/Documents/students1.txt")
records = Rdd1.map(split)

schema = StructType(
	[
		StructField('id', LongType(), False),
		StructField('name', StringType(), False),
		StructField('gender', StringType(), True),
		StructField('grp1', StringType(), True),
		StructField('marks', StringType(), True)
	]
)



df1 = sql.createDataFrame(records, schema)

#df2 = df1.select(df1.name, df1.marks).filter(df1.marks > 80)

#df1.groupby("grp1").count().show()

#df1.groupby("name").sum("marks").show()

#df1.groupby("grp1").sum("id","marks").show()

df1.select("name",grd("marks").alias("Grade")).show()

#df2.show()
print("====================")
print(df1.dtypes)

