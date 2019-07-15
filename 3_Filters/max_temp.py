from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("max_temp")
sc = SparkContext(conf = conf)

def parseData(lines):
    fields = lines.split(",")
    station = fields[0]
    entryType = fields[2]
    temp = fields[3]
    return(station, entryType, temp)    

temp_rdd = sc.textFile("C:\\Sparkcourse\\3_MinMax_Temp\\1800.csv")
parsedRdd = temp_rdd.map(parseData)
filterRdd = parsedRdd.filter(lambda x: x[1] in 'TMAX')
result = filterRdd.collect()
for value in result:
    print(value)
#print(filterRdd.take(5))
