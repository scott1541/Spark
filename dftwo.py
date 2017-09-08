from pyspark import SparkConf, SparkContext
from pyspark.sql import *

conf1 = SparkConf()

sc = SparkContext(conf = conf1)
sql = SQLContext(sc)

Rdd1 = sc.textFile("file:///home/cloudera/Documents/students2.txt")
records = Rdd1.map(lambda x: x.split(","))

df1 = sql.createDataFrame(records, ['ID', 'Subject', 'Marks'])

print("---  print schema")

df1.printSchema()

df1.show()

df1.select(df1.ID,df1.Subject.alias("Boom"), df1.Marks).filter(df1.Marks > 60).show()

df1.select(df1.Subject).filter(df1.Marks > 70).show()

