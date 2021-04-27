from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

# Creating schema when reading customers-orders...
schema = StructType([StructField("CustomerID", IntegerType(), True),\
                 StructField("ItemID", FloatType(), True),\
                 StructField("AmountSpent", FloatType(), True)])


# Reading file as dataframe..
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

# Selecting columns we need..
spentByCustomer = df.select("CustomerID", "AmountSpent")

# Aggregate to find sum of amount spent and showing results..
totalSpent = spentByCustomer.groupBy("CustomerID").agg(func.round(func.sum("AmountSpent"), 2).alias("TotalSpent")).orderBy("TotalSpent")

totalSpent.show(totalSpent.count())

spark.stop()
