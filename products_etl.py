from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import desc

conf1 = SparkConf()

sc = SparkContext(conf = conf1)
sqlC = SQLContext(sc)

def split(x):
	record = x.split(",")
	return record[0], record[1], int(record[2]),float(record[3])

Rdd1 = sc.textFile("file:///home/cloudera/Documents/ETL/sales.txt").map(split)
Rdd2 = sc.textFile("file:///home/cloudera/Documents/ETL/product.txt").map(lambda x: x.split(","))
Rdd3 = sc.textFile("file:///home/cloudera/Documents/ETL/cat.txt").map(lambda x: x.split(","))
Rdd4 = sc.textFile("file:///home/cloudera/Documents/ETL/subcat.txt").map(lambda x: x.split(","))

schemaSales = StructType(
	[	
		StructField('salesid', StringType(), False),
		StructField('productid', StringType(), False),
		StructField('quantity', LongType(), False),
		StructField('price', FloatType(), True)
	]
)

schemaProduct = StructType(
	[	
		StructField('productid', StringType(), False),
		StructField('subid', StringType(), False),
		StructField('name', StringType(), False)
	]
)

schemaCat = StructType(
	[	
		StructField('categoryid', StringType(), False),
		StructField('category', StringType(), False)
	]
)
schemaSubCat = StructType(
	[	
		StructField('categoryid', StringType(), False),
		StructField('subid', StringType(), False),
		StructField('subcat', StringType(), False)
	]
)

dfProduct = sqlC.createDataFrame(Rdd2, schemaProduct)
dfSales = sqlC.createDataFrame(Rdd1, schemaSales)
dfCategory = sqlC.createDataFrame(Rdd3, schemaCat)
dfSubCat = sqlC.createDataFrame(Rdd4, schemaSubCat)

#dfProduct.show()
#dfSales.show()
#dfCategory.show()
#dfSubCat.show()

dfProduct.registerTempTable("Product")
dfSales.registerTempTable("Sales")
dfCategory.registerTempTable("Category")
dfSubCat.registerTempTable("SubCategory")

#Quantity sold 
x = sqlC.sql("SELECT Sales.productid as ProductID, Product.name as Name, Sales.quantity as Quantity_Sold FROM Sales INNER JOIN Product ON Sales.productid=Product.productid ORDER BY Sales.quantity DESC")

#No. of sales
y = sqlC.sql("SELECT Sales.productid as ProductID, count(*) as Sales FROM Sales INNER JOIN Product ON Sales.productid=Product.productid GROUP BY Sales.productid ORDER BY count(*) DESC")

# Throws error
#y2 = sqlC.sql("SELECT Product.productid, Product.name, (SELECT count(*) as Sales FROM Sales INNER JOIN Product ON Sales.productid=Product.productid GROUP BY Sales.productid) as No_Sales FROM Product ORDER BY No_Sales DESC")

#Other attempts at queries
#x2 = sqlC.sql("SELECT Sales.productid, Product.name, (SELECT count(*) as count FROM Sales INNER JOIN Product ON Sales.productid=Product.productid ORDER BY Sales.quantity DESC)")
#z = sqlC.sql("SELECT * FROM Product WHERE value IN (SELECT Sales.productid as ProductID, Product.name as Name, #Sales.quantity as Quantity_Sold FROM Sales INNER JOIN Product ON Sales.productid=Product.productid ORDER BY #Sales.quantity DESC)") 




print("Sales Quantiy")
print("====================")
x.show()
print("No. of Sales")
print("====================")
y.show()
