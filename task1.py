"""
Imports the forsquare dataset and does the time calculation,
it saves the output as many different partitions (in my case, 58 partitions).
- johan
"""

from datetime import datetime
from datetime import timedelta
from pyspark import SparkConf
from pyspark import SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

foursquare_data = sc.textFile("foursquare-data/dataset_TIST2015.tsv")
#cities_data = sc.textFile("foursquare-data/dataset_TIST2015_Cities.txt")

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

header = foursquare_data.first()
headless = foursquare_data.filter(lambda x: x != header)
new_rdd = headless.map(set_time)
new_rdd.saveAsTextFile("foursquare-data/foursquare_edit")
