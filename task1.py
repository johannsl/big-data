from datetime import datetime
from datetime import timedelta
import json
from pyspark import SparkConf
from pyspark import SparkContext

#TODO Make this efficient, should be saves as a tsv

#new_times = {}

conf = (SparkConf()
         .setMaster("local[2]")
         .setAppName("foursquare")
         .set("spark.executor.memory", "2g"))
sc = SparkContext(conf = conf)

foursquare-data-cities = sc.textFile('./foursquare-data/dataset_TIST2015_Cities.txt')
foursquare-data = sc.textFile('./foursquare-data/dataset_TIST2015.tsv')

#def printer(data):
#    print data

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
#data0.foreach(printer)
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
