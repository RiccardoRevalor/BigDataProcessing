import sys, os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, split, when, hour, dayofweek, concat_ws
from datetime import datetime

conf = SparkConf().setAppName("Name of my application")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.getOrCreate()
sep = '\t'
crit_thres = 0.4

stationsInput = spark.read.text("input/stations.csv",
                                format="csv",
                                sep=sep,
                                header=True,
                                inferSchema=True)

registerInput = spark.read.text("input/registerSample.csv",
                                format="csv",
                                sep=sep,
                                header=True,
                                inferSchema=True)

#Filter stations
'''
with RDD I did:
def filterStations(line):
    #stationId\ttimestamp\tusedslots\tfreeslots
    fields = line.split(sep)
    if (fields[2] == '0' and fields[3] == '0'):
        return False
    return True

Now I have to translate this to a filter in a DataFrame
'''
stationsInput = stationsInput.filter("usedslots != 0 and freeslots != 0")



#function udf to get the right timestamp

#create associated table
registerInput.createOrReplaceTempView("register")

def get_timestamp(t):
    date = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    dayOfWeek = date.strftime("%A")
    hour = date.hour
    return dayOfWeek + "," + str(hour)

get_timestamp_udf = spark.udf.register("GET_TIMESTAMP", get_timestamp)

#return ((stationId, timestamp), (n, m))
#if freeslots == 0 -> n=1 otherwise n = 0
#m=1 each time
registerInput = registerInput.sql("""SELECT station, GET_TIMESTAMP(timestamp) as timestamp,
                                    CASE WHEN freeslots = 0 THEN 1 ELSE 0 END as n,
                                    1 as m
                                    FROM register""")

critical = registerInput.sql("""SELECT station, timestamp, SUM(n) / SUM(m) as critical_rate
                                    FROM register
                                    GROUP BY station, timestamp
                                    HAVING critical_rate >= {}""".format(crit_thres))

stations = stationsInput.select("station", "lon", "lat")

#Now, I have to use just station as a key and implement the custom reducer by key:
'''
now we have ((stationId, timestamp), criticality)
we have to select the most critical timestamp for each station
If there are two or
more timeslots characterized by the highest criticality value for the same station,
select only one of those timeslots. Specifically, select the one associated with the
earliest hour. If also the hour is the same, consider the lexicographical order of the
name of the week day. 
'''
critical.createOrReplaceTempView("critical")
critical = critical.sql("""SELECT station, timestamp, critical_rate
                                FROM critical
                                WHERE (station, critical_rate) IN SELECT(station, MAX(critical_rate)
                                                                    FROM critical
                                                                    GROUP BY station)""")

#Join critical table qith stations table
stations.createOrReplaceTempView("stations")

result = critical.sql("""SELECT c.station, c.timestamp, c.critical_rate, s.lon, s.lat
                      FROM critical c, stations s
                      WHERE c.station = s.station""")



spark.stop()
sc.stop()