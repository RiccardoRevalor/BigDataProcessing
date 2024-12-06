import sys
from pyspark import SparkConf, SparkContext
from datetime import datetime

conf = SparkConf().setAppName("Name of my application")
sc = SparkContext(conf = conf)
sep = '\t'

stationsInput = sc.textFile("input/stations.csv")
registerInput = sc.textFile("input/registerSample.csv")

#remove headers of both files
stationsInput = stationsInput.filter(lambda x: "id" not in x)
registerInput = registerInput.filter(lambda x: "station" not in x)

#create the KML file
kmlOutputFile = open("output/critical_stations.kml", "w")
kmlOutputFile.write('<kml xmlns="http://www.opengis.net/kml/2.2"><Document>')

crit_thres = 0.4

def filterStations(line):
    #stationId\ttimestamp\tusedslots\tfreeslots
    fields = line.split(sep)
    if (fields[2] == '0' and fields[3] == '0'):
        return False
    return True

registerRDD = registerInput.filter(filterStations)

def mapRegRDD(line):
    #stationId\ttimestamp\tusedslots\tfreeslots
    #return ((stationId, timestamp), (n, m))
    fields = line.split(sep)
    date = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")
    dayOfWeek = date.strftime("%A")
    hour = date.hour
    timestamp = dayOfWeek + "," + str(hour)

    #map to ((stationId, timestamp), (n, m))
    #if freeslots == 0 -> n=1 otherwise n = 0
    #m=1 each time
    freeSlots = int(fields[3])
    if freeSlots == 0:
        return ((fields[0], timestamp), (1, 1))
    
    return ((fields[0], timestamp), (0, 1))

def mapStatRDD(line):
    #stationId\tname\tlon\tlat
    #return (stationId, (lon, lat))
    fields = line.split(sep)
    return (fields[0], (float(fields[1]), float(fields[2])))

stationsRDD = stationsInput.map(mapStatRDD).cache()

criticalRDD = registerRDD.map(mapRegRDD)\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
    .mapValues(lambda x: float(x[0]) / float(x[1]))\
    .filter(lambda x: x[1] >= crit_thres).cache()

'''
now we have ((stationId, timestamp), criticality)
we have to select the most critical timestamp for each station
If there are two or
more timeslots characterized by the highest criticality value for the same station,
select only one of those timeslots. Specifically, select the one associated with the
earliest hour. If also the hour is the same, consider the lexicographical order of the
name of the week day. 
'''

def customReducerByKey(x, y):
    #x, y are keys in this format (timestamp, criticality)
    if (float(x[1]) > float(y[1])):
        return x
    elif (float(x[1]) < float(y[1])):
        return y
    
    #if the criticality is the same tehn select the one with the earliest hour
    h1 = int(x[0].split(',')[1])
    h2 = int(y[0].split(',')[1])
    if h1 < h2:
        return x
    elif h1 > h2:
        return y
    
    #if the hour is the same then consider the lexicographical order of the name of the week day
    d1 = x[0].split(',')[0]
    d2 = y[0].split(',')[0]
    if d1 < d2:
        return x
    return y

#Map criticalRDD to ((stationId, (timestamp, criticality)))
#Use custom reducer to select the most critical timestamp for each station
#then join with stationsRDD to get the lon, lat for each stations (it's a left join) -> (stationId, ((timestamp, criticality), (lon, lat)))
criticalityTSPerStationRDD = criticalRDD.map(lambda t: (t[0][0], (t[0][1], t[1])))\
    .reduceByKey(customReducerByKey)\
    .join(stationsRDD)


def mapToKMLPlacemark(record):
    #record come as: (stationId, ((timestamp, criticality), (lon, lat)))
    #return the KML Placemark
    '''
    <Placemark><name>44</name><ExtendedData><Data
    name="DayWeek"><value>Mon</value></Data><Data
    name="Hour"><value>3</value></Data><Data
    name="Criticality"><value>0.5440729483282675</value></Data></ExtendedData><
    Point><coordinates>2.189700,41.379047</coordinates></Point></Placemark>
    '''
    pl = "<Placemark><name>" + record[0] + "</name><ExtendedData><Data " + \
        "name=\"DayWeek\"><value>" + record[1][0][0].split(',')[0] + "</value></Data><Data " + \
        "name=\"Hour\"><value>" + record[1][0][0].split(',')[1] + "</value></Data><Data " + \
        "name=\"Criticality\"><value>" + str(record[1][0][1]) + "</value></Data></ExtendedData><" + \
        "Point><coordinates>" + str(record[1][1][0]) + "," + str(record[1][1][1]) + "</coordinates></Point></Placemark>"
    
    return pl

#map every record in the RDD to a KML Placemark and write it to the file
kmlOutputFile.write(criticalityTSPerStationRDD.map(mapToKMLPlacemark).collect())

##add end of the KML file
kmlOutputFile.write('</Document></kml>')


    


sc.stop()