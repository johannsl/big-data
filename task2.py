"""
Import and edit the result from task1.py.
task1.py's output can be found in foursquare-data/foursquare_edit/part-xxxxx
- johan
"""

from datetime import datetime
from datetime import timedelta
from pyspark import SparkConf
from pyspark import SparkContext

# Create a spark context
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

# Print some SparkConf variables
print "\nSparkConf variables: \n", conf.toDebugString()

# Import data
foursquare_data = sc.textFile("foursquare-data/foursquare_edit/part-00000")

# Provoke action in dataset
data_type = type(foursquare_data) 
first = foursquare_data.first()
count = foursquare_data.count()

# Print some data information
print "\nfoursquare_data filetype: ", data_type
print "\nFirst element in the dataset: ", first
print "\nNumber of elements: ", count

# Save the data
#foursquare_data.saveAsHadoopDataset("foursquare/hadoop_dataset")

# Stop the spark context
sc.stop()
