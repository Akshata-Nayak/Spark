from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Rating_Counter")
sc = SparkContext(conf = conf)

def mapMovieDic():
    movie={}
    with open("C:\\Sparkcourse\\1_Counter\\ml-100k\\u.item", 'r') as mm:
        for data in mm:
            fields=data.split('|')
            movie_id=int(fields[0])
            movie_name=fields[1]

            movie[movie_id] = movie_name
    
    return(movie)

def parsedata(lines):
    fields=lines.split()
    movie_id2=int(fields[1])
    return(movie_id2)

filelines = sc.textFile("C:\\Sparkcourse\\1_Counter\\ml-100k\\u.data")
movieRdd = filelines.map(parsedata)
mapMovieRdd = movieRdd.map(lambda x:(x,1)).reduceByKey(lambda x, y: (x+y))
mapMovieSortedRdd = mapMovieRdd.map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))

moviesDict = sc.broadcast(mapMovieDic())
#TypeError: 'Broadcast' object is not callable
#watchedMovieRDD=mapMovieSortedRdd.map(lambda x: (moviesDict(x[0]), x[1]))

# TypeError: 'dict' object is not callable
# watchedMovieRDD=mapMovieSortedRdd.map(lambda x: (moviesDict.value(x[0]), x[1]))

watchedMovieRDD=mapMovieSortedRdd.map(lambda x: (moviesDict.value[x[0]], x[1]))
watchedMovie=watchedMovieRDD.collect()

for values in watchedMovie:
    print('{}'.format(values[0]), ': ' '{}'.format(values[1]))