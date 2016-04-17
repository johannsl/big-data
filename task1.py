from datetime import datetime
from datetime import timedelta
from pyspark import SparkConf
from pyspark import SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

foursquare_data = sc.textFile("../big-data/foursquare-data/dataset_TIST2015.tsv")
cities_data = sc.textFile("../big-data/twitter-data/dataset_TIST2015_Cities.txt")

foursquare_data = sc.textFile("./foursquare-data/dataset_TIST2015.tsv")
cities_data = sc.textFile("./twitter-data/dataset_TIST2015_Cities.txt")

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
#new_rdd.count()

# To be worked on
#def time_printer(data):
#    NotImplemented 
#
#def time_calc(data):
#    date = datetime.strptime(data.split("\t")[3],'%Y-%m-%d %H:%M:%S')
#    delta = int(data.split("\t")[4])
#    total = datetime.strftime(date + timedelta(minutes=delta))
#    
#    
#
#header = data0.first()
#data0 = data0.filter(lambda x:x !=header)
#

#data0.foreach(time_printer)
#data0.foreach(time_calc)

#temp0 = temp0.take(10)

#for line in temp0:
	#columns = line.split("\t")
	#time0 = time_calc(columns[3])
	#print temp0
	#print time0
	#time1 = time0 + timedelta(0, int(columns[4])*60)
	#print int(columns[4])/60
	#print time1
	#new_times[columns[0]] = time1
#print new_times

#with open("./times.json", "w") as f:
#	json.dump(new_times, f)
