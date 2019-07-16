from pyspark import SparkConf, SparkContext

def parsedata(lines):
    fields = lines.split(",")
    customer_id = fields[0]
    amt_spent = float(fields[2])
    return(customer_id, amt_spent)

config = SparkConf().setMaster("local").setAppName("Amount_Spent_per_cus")
sc = SparkContext(conf = config)

customer_orders=sc.textFile("C:\\Sparkcourse\\5_AmountSpentPerCustomer\\customer-orders.csv")
custDataRDD=customer_orders.map(parsedata)
amtPerCustRDD=custDataRDD.reduceByKey(lambda x, y: x+y)
sortedAmtRDD=amtPerCustRDD.map(lambda x: (x[1], x[0])).sortByKey()
sortedAmt=sortedAmtRDD.map(lambda x: (x[1], x[0])).collect()

for custID, amt in sortedAmt:
    print('{} {:.2f}'.format(custID, amt))