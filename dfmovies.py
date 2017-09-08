from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import desc

conf1 = SparkConf()

sc = SparkContext(conf = conf1)
sql = SQLContext(sc)

def splitRatings(x):
	record = x.split("\t")
	return int(record[0]), int(record[1]), int(record[2]), record[3]
	
def splitMovies(x):
	record = x.split("|")
	return int(record[0]), record[1]

def combine(x,y):

	return 

Rdd1 = sc.textFile("file:///home/cloudera/Documents/u.data")
Rdd2 = sc.textFile("file:///home/cloudera/Documents/u.item")

ratRecords = Rdd1.map(splitRatings)
movRecords = Rdd2.map(splitMovies)

schemaR = StructType(
	[	
		StructField('userid', LongType(), False),
		StructField('movieid', LongType(), False),
		StructField('movierating', LongType(), True),
		StructField('timestamp', StringType(), True)
	]
)

schemaM = StructType(
	[	
		StructField('movieid', LongType(), False),
		StructField('moviename', StringType(), False)
	]
)

dfMovies = sql.createDataFrame(movRecords, schemaM)
dfRatings = sql.createDataFrame(ratRecords, schemaR)

#counts = dfRatings.groupby("movieid").count()
#df = df.join(counts, df.movieid == counts.movieid).show()

#dfMovies.show()

#dfRatings.show()

df1 = dfMovies.join(dfRatings, dfMovies.movieid == dfRatings.movieid).select(dfMovies.moviename.alias("Movies"), dfRatings.movierating.alias("Rating"))

print("===========================")
print("Highest movie ratings:")

df1.groupby("Movies").avg("Rating").sort(desc("avg(Rating)")).show(20)
#df1.groupby("moviename").avg("movierating").sort(desc("avg(movierating)")).show(20)




