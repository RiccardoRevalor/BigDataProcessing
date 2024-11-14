#cmd to execute locally: spark-submit --deploy-mode client --master local solution.py <input> <output>

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


if len(sys.argv) != 3:
    print("Usage: spark-submit <python_file_of_the_job> <input_folder> <output_folder>")
    sys.exit(1)

inputPath = sys.argv[1]
outputPath = sys.argv[2]
#create two subpaths for the output of task 1 and 2
outputPath1 = outputPath + '/task1'
outputPath2 = outputPath + '/task2'

#task 1
inputRDD = sc.textFile(inputPath)




#don't take the first line
header = inputRDD.first()
inputRDD = inputRDD.filter(lambda l: l != header)

def userIdProductId(l):
    fields = l.split(',')
    return (fields[2], fields[1])

userProductsRDD = inputRDD.map(userIdProductId)

#for each keu (userId), collect the list of distinct products
#the value of groupByKey is an iterable, so we convert it to a set (to fecth distinct values) and then list

userProductsRDD = userProductsRDD.groupByKey()\
.mapValues(lambda p: list(set(p))).cache() #didn't use distinct to avoid shuffling !!!

#save in the output folder
userProductsRDD.map(lambda t: t[0] + "\t" + ",".join(t[1]))\
.saveAsTextFile(outputPath1)





spark.stop()
sc.stop()