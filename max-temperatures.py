from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

#Reading csv file which represents the weather in Paris and Prague in 1800..
lines = sc.textFile("file:///SparkCourse/1800.csv")

# Creating function to parse and extract the columns we need..
def parseLines(line):
    columns = line.split(',')
    stationID = columns[0]
    entryType = columns[2]
    temperature = float(columns[3])
    return (stationID, entryType, temperature)

parsedLines = lines.map(parseLines) # Applying function for every rows..
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1]) # Taking maximum temperatures only..
stationTemps = maxTemps.map(lambda x: (x[0], x[2])) # After taking max temps only, taking out the "TMAX" column..
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y)) # Taking only maximum value for each station in the dataset..
results = maxTemps.collect() # Collecting data for spark..


for i in results:
    print(i[0] + "\t{:.2f}C".format(i[1]))
