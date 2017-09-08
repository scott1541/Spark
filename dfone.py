from pyspark import SparkConf, SparkContext
from pyspark.sql import *

conf1 = SparkConf()

sc = SparkContext(conf = conf1)
sql = SQLContext(sc)

Rdd1 = sc.textFile("file:///home/cloudera/Documents/students2.txt")
records = Rdd1.map(lambda x: x.split(","))

df1 = sql.createDataFrame(records)

print("--- df1.printSchema")
df1.printSchema()

print("--- df1.show")
df1.show(df1.count())
#print(df1.count())
#df1.select("_1","_3").show()
df1.select(df1._3).show()

x = df1.select(df1._3).filter(df1._3 >= 70)
y = df1.filter((df1._3 > 50) & (df1._3 < 80)).select("_1", "_3")
z = df1.filter(df1._3.between(50,80) & (df1._2 == "Chemistry")).select("_1", "_3")
z1 = df1.filter(df1._3.between(45,90) & ((df1._2 == "Chemistry")|(df1._2 == "Physics"))).select(df1._1.alias("ID"), df1._3.alias("Marks"),(df1._3*0.6666).alias("Percentage"))

#x.show()
#y.show()
z1.show()

#df1.groupby()

#print("Records for Chemistry or Physics: ", z.count())


