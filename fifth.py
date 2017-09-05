from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("Fourth program")

sc = SparkContext(conf = conf1)

Rdd1 = sc.textFile("trainers.txt")
Rdd2 = sc.textFile("trainees.txt")