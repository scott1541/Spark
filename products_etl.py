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
x = sqlC.sql("SELECT Product.name as Name, sum(Sales.quantity) as Quantity_Sold FROM Sales INNER JOIN Product ON Sales.productid=Product.productid GROUP BY name ORDER BY Quantity_Sold DESC")

#No. of sales
y = sqlC.sql("SELECT Product.name as Name, count(*) as No_Sales FROM Sales INNER JOIN Product ON Sales.productid=Product.productid GROUP BY name ORDER BY No_Sales DESC")

#Quantity sold per Subcategory
z = sqlC.sql("SELECT subcat as SC_Name, sum(Sales.quantity) as Quantity_Sold FROM Sales INNER JOIN Product ON Sales.productid=Product.productid INNER JOIN (SELECT subcat, subid FROM SubCategory) sq2 ON (sq2.subid = Product.subid) GROUP BY subcat ORDER BY Quantity_Sold DESC")

#Quantity sold per category
w = sqlC.sql("SELECT category as Cat_Name, sum(Sales.quantity) as Quantity_Sold FROM Sales INNER JOIN Product ON Sales.productid=Product.productid INNER JOIN (SELECT subcat, subid, categoryid FROM SubCategory) sq2 ON (sq2.subid = Product.subid) INNER JOIN (SELECT category, categoryid FROM Category) sq3 ON (sq3.categoryid = sq2.categoryid) GROUP BY category ORDER BY Quantity_Sold DESC")

#Attempted using all subqueries but keeps throwing error...
#x2 = sqlC.sql("SELECT category as Cat_Name, sum(Sales.quantity) as Quantity_Sold FROM Sales INNER JOIN (SELECT name, productid, subid FROM Product) sq1 ON (Sales.productid=sq1.productid) INNER JOIN (SELECT subcat, subid, categoryid FROM SubCategory) sq2 ON (sq2.subid = Product.subid) INNER JOIN (SELECT category, categoryid FROM Category) sq3 ON (sq3.categoryid = sq2.categoryid) GROUP BY category ORDER BY Quantity_Sold DESC")



print("Sales Quantity")
print("====================")
x.show()
print("No. of Sales")
print("====================")
y.show()
print("Quantity sold per Subcategory")
print("====================")
z.show()
print("Quantity sold per Category")
print("====================")
w.show()

