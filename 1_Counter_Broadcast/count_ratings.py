from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Rating_Counter")
sc = SparkContext(conf = conf)

filelines = sc.textFile("C:\\Sparkcourse\\1_Counter\\ml-100k\\u.data")
ratings=filelines.map(lambda x:x.split()[2])
ratingsCountDic=ratings.countByValue()

sortedRatings=collections.OrderedDict(sorted(ratingsCountDic.items()))
#print(sortedRatings)

for rate, cnt in sortedRatings.items():
    print('{} {}'.format(rate, cnt))