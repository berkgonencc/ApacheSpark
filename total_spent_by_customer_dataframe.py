from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

schema = StructType([StructField("CustomerID", IntegerType(), True),\
                 StructField("ItemID", FloatType(), True),\
                 StructField("AmountSpent", FloatType(), True)])


# Reading file as dataframe..
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

# Selecting columns we need..
spentByCustomer = df.select("CustomerID", "AmountSpent")

# Aggregate to find sum of amount spent..
totalSpent = spentByCustomer.groupBy("CustomerID").agg(func.round(func.sum("AmountSpent"), 2).alias("TotalSpent")).orderBy("TotalSpent")

# Collecting format and printing results..
results = totalSpent.collect()

for i in results:
    print(str(i[0]) + ":\t" + str(i[1]))

spark.stop()
