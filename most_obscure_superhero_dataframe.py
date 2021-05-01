from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True ),\
                     StructField("name", StringType(), True)])

# Reading txt files..
names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")
lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# Creating columns from marvel-graph.txt..
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
            .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)\
            .groupBy("id").agg(func.sum("connections").alias("connections"))

# Finding minumum connections..
minCount = connections.agg(func.min("connections")).first()[0]
minConnections = connections.filter(func.col("connections") == minCount)

# Joining two tables to get names from names dataframe..
mostObscure = minConnections.join(names, "id")

# Showing results..
mostObscure.select("name").show()
