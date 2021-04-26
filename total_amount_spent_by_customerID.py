from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountSpentByCustomerID")
sc = SparkContext(conf = conf)

# Splitting the comma separated dataset and get required columns..
def parseLines(line):
    columns = line.split(',')
    customerID = int(columns[0])
    amountSpent = float(columns[2])
    return (customerID, amountSpent)

# The csv file have 3 columns customerID, itemID, and the amount spent respectively..
lines = sc.textFile("file:///sparkcourse/customer-orders.csv")

rdd = lines.map(parseLines)
totalsByCustomerID = rdd.reduceByKey(lambda x, y: x + y)
sortByAmountSpent = totalsByCustomerID.map(lambda x: (x[1], x[0])).sortByKey()
result = sortByAmountSpent.collect()

for i in result:
    totalspent = i[0]
    customer = i[1]
    print(customer , ":\t{:.2f}".format(totalspent))
