from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('FriendByAge')
sc = SparkContext(conf = conf)



def fetchAgeCnt(line):
    fields = line.split(",")
    age = int(fields[2])
    cnt = int(fields[3])
    return (age, cnt) 

friendsData=sc.textFile("C:\\Sparkcourse\\2_Friends\\fakefriends.csv")
rddDataset = friendsData.map(fetchAgeCnt) 

rdd_mapVal = rddDataset.mapValues(lambda x:(x,1))
rdd_redKey = rdd_mapVal.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avgAgeFrns = rdd_redKey.mapValues(lambda x: x[0]/x[1])
result = avgAgeFrns.collect()

for values in result:
    print('{} {:.2f}'.format(values[0], values[1]))