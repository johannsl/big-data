from datetime import datetime
from datetime import timedelta
from math import *
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
    foursquare_line.append(city)
    foursquare_line.append(country)
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

# Import data
cities_data = sc.textFile("../foursquare-data/dataset_TIST2015_Cities.txt",
                            use_unicode=False)
foursquare_data = sc.textFile("../foursquare-data/dataset_TIST2015.tsv",
                                use_unicode=False)

# Provoke action in dataset and print results
time0 = datetime.now()

# City data set
print "\nMapping cities - removing excess ...",
cities_data = cities_data.map(remove_excess)
cities_collection = cities_data.collect()
time1 = datetime.now()
print "\nCities removing excess done", time1-time0

# Foursquare data set
print "\nFiltering foursquare - removing header ..."
foursquare_data_header = foursquare_data.first()
foursquare_data = foursquare_data.filter(lambda x: x != foursquare_data_header)
time2 = datetime.now()
print "\nFoursquare removing header done", time2-time1

print "\nFirst foursquare - finding a sample ..."
first = foursquare_data.first()
time3 = datetime.now()
print "\nFoursquare finding a sample done", time3-time2, "\n", first

print "\nMapping foursquare - setting date and time ..."
foursquare_data = foursquare_data.map(set_time)
time4 = datetime.now()
print "\nFoursquare setting date and time done", time4-time3

print "\nFirst foursquare - checking sample ..."
first = foursquare_data.first()
time5 = datetime.now()
print "\nFoursquare checking sample done", time5-time4, "\n", first




#foursquare_data = foursquare_data.map(assign_location)
#fq_data_type = type(foursquare_data)
#fq_count = foursquare_data.count()
#fq_first = foursquare_data.first()
#fq_top_5 = foursquare_data.top(5)



# Print some data information
#print "\n### CITIES_DATA ###"
#print "\ncities_data filetype: ", c_data_type
#print "\nNumber of elements: ", c_count
#print "\nFirst element in the dataset: ", c_first
#print "\nTop 5 elements in the dataset: ", c_top_5
#print "\ncities_collection type: ", cities_collection_type
#print "\n### FOURSQUARE_DATA ###"
#print "\nfoursquare_data filetype: ", fq_data_type
#print "\nNumber of elements: ", fq_count
#print "\nFirst element in the dataset: ", fq_first
#print "\nTop 5 elements in the dataset: ", fq_top_5

# Stop the spark context
sc.stop()
