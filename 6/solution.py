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




#TASK 2
from itertools import combinations
def ProductPairs(t):
    products = t[1]
    pairs = list(combinations(products, 2)) #generate all possible pairs of products bought by the user
    #pairs = [(p1, p2), (p1, p3), (p2, p3), ...]

    #at the end I wanna have a (product1 + "," + product2, 1) tuple
    res =  [(tupla[0] + "," + tupla[1], 1) for tupla in pairs]
    return res

#(p1 + "," + p2, 1) + (p1 + "," + p2, 1) + ... = (p1 + "," + p2, 2)
ProductPairsRDD = userProductsRDD.flatMap(ProductPairs)\
.reduceByKey(lambda a, b: a + b).cache()    #count the number of times each pair of products was bought together

print("Number of pairs:\n", ProductPairsRDD.count())
print("First 10 pairs:\n", ProductPairsRDD.take(10))

#sort them by decreasing order of frequency
sortedProductPairsRDD = ProductPairsRDD.filter(lambda t: t[1] > 1)\
.sortBy(lambda t: t[1], ascending=False)\
.map(lambda t: t[0] + "\t" + str(t[1]))\
.saveAsTextFile(outputPath2) 




spark.stop()
sc.stop()