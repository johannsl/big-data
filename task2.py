"""
Import and edit the result from task1.py.
task1.py's output can be found in foursquare-data/foursquare_edit/part-xxxxx
- johan
"""

from datetime import datetime
from datetime import timedelta
from pyspark import SparkConf
from pyspark import SparkContext

# Script functions to edit data
def assignLocation(data):
    line = data.split("\t")
	
    
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
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

# Print some SparkConf variables
print "\nSparkConf variables: ", conf.toDebugString()
print "\nSparkConf id: ", sc.applicationId
print "\nUser: ", sc.sparkUser()
print "\nVersion: ", sc.version

# Import data
foursquare_data = sc.textFile("foursquare-data/foursquare_edit/part-00000",
                                use_unicode=False)
cities_data = sc.textFile("foursquare-data/dataset_TIST2015_Cities.txt")

# Provoke action in dataset
fq_data_type = type(foursquare_data) 
fq_count = foursquare_data.count()
fq_first = foursquare_data.first()
fq_top_5 = foursquare_data.top(5)

c_data_type = type(cities_data) 
c_count = cities_data.count()
c_first = cities_data.first()
c_top_5 = cities_data.top(5)

# Print some data information
print "\n### FOURSQUARE_DATA ###"
print "\nfoursquare_data filetype: ", fq_data_type
print "\nNumber of elements: ", fq_count
print "\nFirst element in the dataset: ", fq_first
print "\nTop 5 elements in the dataset: ", fq_top_5

print "\n### CITIES_DATA ###"
print "\ncities_data filetype: ", c_data_type
print "\nNumber of elements: ", c_count
print "\nFirst element in the dataset: ", c_first
print "\nTop 5 elements in the dataset: ", c_top_5

# Save the data
#foursquare_data.saveAsHadoopDataset("foursquare/hadoop_dataset")

# Stop the spark context
sc.stop()
