#cmd to execute locally: spark-submit --deploy-mode client --master local solution.py <prefix> <input> <output>

import sys
import findspark
findspark.init()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('lab05').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

#while True:
#    if sc._jsc.sc().isStopped():
#        break


if len(sys.argv) != 4:
    print("Usage: spark-submit <python_file_of_the_job> <prefix> <input_folder> <output_folder>")
    sys.exit(1)

prefix = sys.argv[1]
inputPath = sys.argv[2]
outputPath = sys.argv[3]

#create three subpaths for the output of task 1, 2, and 3
outputPath1 = outputPath + '/task1'
outputPath2 = outputPath + '/task2'
outputPath3 = outputPath + '/task3'

#TASK 1
inputRDD = sc.textFile(inputPath)

def WordFreq(l):
    #return (Word, Frequency)
    fields = l.split('\t')
    return (fields[0], int(fields[1]))

prefixRDD = inputRDD.map(WordFreq)\
.filter(lambda t: t[0].startswith(prefix)).cache()

#save in the output folder
prefixRDD.map(lambda t: t[0] + "\t" + str(t[1]))\
.saveAsTextFile(outputPath1)

#count the number of words
numWords = prefixRDD.count()

#select the highest frequency
maxFreq = prefixRDD.map(lambda t: t[1]).max()

#print the results
print('Number of words starting with {}: {}\n'.format(prefix, numWords))
print('Maximum frequency of words starting with {}: {}\n'.format(prefix, maxFreq))



#TASK 2
thres = 0.8 * maxFreq

filteredRDD = prefixRDD.filter(lambda t: t[1] > thres).cache()

#save in the output folder
filteredRDD.map(lambda t: t[0])\
.saveAsTextFile(outputPath2)

#count the number of words
numWords = filteredRDD.count()

#print the results
print('Number of words starting with {} and having frequency > 0.8 * maxFreq: {}\n'.format(prefix, numWords))



#TASK 3
def GroupOne(l):
    freq = l[1]
    if 0<=freq<100:
        return ('Group 0', 1)
    elif 100<=freq<200:
        return ('Group 1', 1)
    elif 200<=freq<300:
        return ('Group 2', 1)
    elif 300<=freq<400:
        return ('Group 3', 1)
    elif 400<=freq<500:
        return ('Group 4', 1)
    else:
        return ('Group 5', 1)
    
groupsRDD = prefixRDD.map(GroupOne)\
.reduceByKey(lambda x, y: x+y)\
.map(lambda t: t[0] + "\t" + str(t[1]))\
.saveAsTextFile(outputPath3)
    




#stop everything
spark.stop()
sc.stop()