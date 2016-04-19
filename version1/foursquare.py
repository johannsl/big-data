from datetime import datetime
from datetime import timedelta
from math import *
from operator import add
from operator import itemgetter
from pyspark import SparkConf
from pyspark import SparkContext
import sys

# Script functions to edit data
def set_time(data):
    line = data.split("\t")
    old_date = datetime.strptime(line[3], "%Y-%m-%d %H:%M:%S")
    delta = int(line[4])
    new_date = datetime.strftime(old_date + timedelta(minutes=delta),
                "%Y-%m-%d %H:%M:%S")
    line[3] = new_date
    line[4] = "0"
    result = "\t".join(line)
    return result

def remove_excess(data):
	line = data.split("\t")
	line.pop() # pop the city type
	line.pop() # pop the country fullname
	result = "\t".join(line)
	return result

def assign_location(foursquare_data):
    foursquare_line = foursquare_data.split("\t")
    lat = float(foursquare_line[5])
    lon = float(foursquare_line[6])
    distance = sys.maxint
    city = None
    country = None
    for city_data in cities_collection:
        city_line = city_data.split("\t")
        temp_distance = haversine(lat, lon, 
                                    float(city_line[1]), float(city_line[2]))
        if (temp_distance < distance):
            distance = temp_distance
            city = city_line[0]
            country = city_line[3]
    foursquare_line.append(country)
    foursquare_line.append(city)
    result = "\t".join(foursquare_line)
    return result
    
def haversine(lat1, lon1, lat2, lon2):
	# convert decimal degrees to radians
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a))
	r = 6371 # Radius of earth in kilometers. Use 3956 for miles
	return c * r

def foursquare_id(data):
    line = data.split("\t")
    return (line[1], 1)

def foursquare_session(data):
    line = data.split("\t")
    return (line[2], 1)

def foursquare_country(data):
    line = data.split("\t")
    return (line[9], 1)

def foursquare_city(data):
    line = data.split("\t")
    return (line[10], 1)

# Create a spark context
conf = (SparkConf()
         .setMaster("local[2]")
         .setAppName("My app")
         .set("spark.executor.memory", "4g"))
sc = SparkContext(conf = conf)

# Print some SparkConf variables
print "\nSparkConf variables: ", conf.toDebugString()
print "\nSparkConf id: ", sc.applicationId
print "\nUser: ", sc.sparkUser()
print "\nVersion: ", sc.version

# ---- Task 1. ----
# Import data
cities_data = sc.textFile("../foursquare-data/dataset_TIST2015_Cities.txt",
                            use_unicode=False)
# //// Task 1 ////

foursquare_data = sc.textFile("../foursquare-data/dataset_TIST2015.tsv",
                                use_unicode=False)

# Provoke action in dataset and print results
time0 = datetime.now()

# City data set
print "\nMapping cities - removing excess ...\n",
cities_data = cities_data.map(remove_excess)
cities_collection = cities_data.collect()
time1 = datetime.now()
print "\nCities removing excess done", time1-time0, "\n"

# Foursquare data set
print "\nFiltering foursquare - removing header ...\n"
foursquare_data_header = foursquare_data.first()
foursquare_data = foursquare_data.filter(lambda x: x != foursquare_data_header)
time2 = datetime.now()
print "\nFoursquare removing header done", time2-time1, "\n"

print "\nFirst foursquare - finding a sample ...\n"
first = foursquare_data.first()
time3 = datetime.now()
print "\nFoursquare finding a sample done", time3-time2, "\n", first, "\n"

# ---- Task 2. ----
print "\nMapping foursquare - setting date and time ...\n"
foursquare_data_time = foursquare_data.map(set_time)
time4 = datetime.now()
print "\nFoursquare setting date and time done", time4-time3, "\n"
# //// Task 2. ////

print "\nFirst foursquare - checking sample 1 ...\n"
first = foursquare_data_time.first()
time5 = datetime.now()
print "\nFoursquare checking sample 1 done", time5-time4, "\n", first, "\n"

# ---- Task 3. ----
print "\nMap foursquare - assigning locations ...\n"
foursquare_data_location = foursquare_data.map(assign_location)
time6 = datetime.now()
print "\nFoursquare assigning locations done", time6-time5, "\n"
# //// Task 3. ////

print "\nFirst foursquare - checking sample 2 ...\n"
first = foursquare_data_location.first()
time7 = datetime.now()
print "\nFoursquare checking sample 2 done", time7-time6, "\n", first, "\n"

# ---- Task 4. (a) ----
#print "\nReduceByKey foursquare - finding unique users ...\n"
#key_value = foursquare_data.map(foursquare_id)
#users = key_value.reduceByKey(add).count()
time8 = datetime.now()
#print "\nFoursquare finding unique users done", time8-time7, "\n", users, "\n"
# 256307
# //// Task 4. (a) ////

# ---- Task 4. (b) ----
#print "\nCount foursquare - finding total check-ins ...\n"
#check_ins = foursquare_data.count()
time9 = datetime.now()
#print "\nFoursquare finding total check-ins done", time9-time8, "\n", check_ins, "\n"
# 19265256
# //// Task 4. (b) ////

# ---- Task 4. (c) ----
#print "\nReduceByKey foursquare - finding total check-in sessions ...\n"
#key_value = foursquare_data.map(foursquare_session)
#sessions = key_value.reduceByKey(add).count()
time10 = datetime.now()
#print "\nFoursquare finding total check-in sessions done", time10-time9, "\n", sessions, "\n"
# 6338302
# //// Task 4. (c) ////

"""
The next two problems are difficult.
It might be because the mapping is slow (assign_location()).
Or it might be the count() in itself that is slow.

We could try to count approximately (there are ways).
"""

# ---- Task 4. (d) ----
#print "\nReduceByKey foursquare - finding countries represented ...\n"
#key_value = foursquare_data_location.map(foursquare_country)
#countries = key_value.reduceByKey(add).count()
time11 = datetime.now()
#print "\nFoursquare finding countries represented done", time11-time10, "\n", countries, "\n"
# 26 (77)
# //// Task 4. (d) ////

# ---- Task 4. (e) ----
#print "\nReduceByKey foursquare - finding cities represented ...\n"
#key_value = foursquare_data_location.map(foursquare_cities)
#cities = key_value.reduceByKey(add).count()
time12 = datetime.now()
#print "\nFoursquare finding cities represented done", time12-time11, "\n", cities, "\n"
# 413
# //// Task 4. (e) ////

# ---- Task 5. ----
#lol
# //// Task 5. ////

# Stop the spark context
sc.stop()
