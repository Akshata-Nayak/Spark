import re    # Regular Expression
from pyspark import SparkContext, SparkConf

def dataNormalize(text):
    # Error : UnicodeEncodeError: 'charmap' codec can't encode character '\ufffd' in position 2: character maps to <undefined
    # return(text.lower().split(' '))
    
    # Error: TypeError: a bytes-like object is required, not 'str'
    # return(text.encode("utf-8").lower().split(' '))
    # re --> Both patterns and strings to be searched can be Unicode strings as well as 8-bit strings.
    # re.compile -> compiles pattern to pattern object. 
    # /W -> looks for all alphanumeric combinations. Equivalent to [^a-zA-z0-9_]
    # r in re.compile tells regex to escape characters like backslash. \n is literally considered \n and not new line
    # here split is from re.split
    return(re.compile(r"\W+", flags=re.UNICODE).split(text.lower()))

conf = SparkConf().setMaster("local").setAppName("Book_count")
sc = SparkContext(conf = conf)

readFile = sc.textFile("C:\\Sparkcourse\\4_FlatMap_Sort\\Book.txt")
mapWords = readFile.flatMap(dataNormalize)
countWordsRdd = mapWords.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y)
sortedbycount = countWordsRdd.map(lambda x: (x[1], x[0])).sortByKey()  # Interchange Key and Value. Sort by Key

#print(sortedbycount.take(5))
countWords = sortedbycount.collect()

for values in countWords:
    print(values[0], values[1])