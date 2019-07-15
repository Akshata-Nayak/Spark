from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Rating_Counter")
sc = SparkContext(conf = conf)

def parsetempData(lines):
    fields = lines.split(",")
    station = fields[0]
    entryType = fields[2]
    temp = fields[3]
    return(station, entryType, temp)

temprdd = sc.textFile("C:\\Sparkcourse\\3_MinMax_Temp\\1800.csv")
templines = temprdd.map(parsetempData)
temp_min = templines.filter(lambda x: 'TMIN' in x[1])
results = temp_min.collect()
for value in results:
    print(value)