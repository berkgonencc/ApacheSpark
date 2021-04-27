from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Creating Spark Session..
spark = SparkSession.builder.appName("AvgFriendsForEachAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
        .csv("file:///SparkCourse/fakefriends-header.csv")

print("inferred Schema;")
people.printSchema()

# Selecting only age and friends columns..
friendsByAge = people.select("age", "friends")

# Finding average of friends for each age, and using agg function to round numbers..
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("Avg_Friends"))\
            .orderBy("age").show()

spark.stop()
