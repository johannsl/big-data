from datetime import datetime
from datetime import timedelta
from math import *
from operator import add
from operator import itemgetter
from pyspark import SparkConf
from pyspark import SparkContext
import sys

cities_collection = 0
max_value = 0
filter_sessions_col = 0
distance_foursquare_col = 0

# Main function
def main():
    global cities_collection
    global max_value
    global filter_sessions_col
    global distance_foursquare_col   

    # Spark init
    sc = spark_init()

    # Task 1: Data init
    time0 = datetime.now()
    cities_data = sc.textFile("../foursquare-data/dataset_TIST2015_Cities.txt",
                        use_unicode=False)
    foursquare_data = sc.textFile("../foursquare-data/foursquare_edit/part-00000",
                            use_unicode=False)
    foursquare_data_header = foursquare_data.first()
    foursquare_data = foursquare_data.filter(lambda x: x != foursquare_data_header)
    cities_data = cities_data.map(c_init)
    cities_collection = cities_data.collect()
    foursquare_data = foursquare_data.map(f_init)
    time1 = datetime.now()
    print "\nData init", time1-time0, "\n"

    # Task 2: Set times
    #foursquare_data_time = foursquare_data.map(set_time)
    time2 = datetime.now()
    #print "\nSet times", time2-time1, "\n"

    # Task 3: Set location 
    foursquare_data_locations = foursquare_data.map(assign_location)
    time3 = datetime.now()
    #take_5 = foursquare_data_locations.take(5)
    #print take_5
    print "\nSet location", time3-time2, "\n"
    
    # Task 4a: Find unique users
    #key_value = foursquare_data.map(lambda x: (x[1], 1))
    #users = key_value.reduceByKey(add)
    #users_count = users.count()
    time4 = datetime.now()
    #print "\nFind unique users", time4-time3, "\n", users_count, "\n"
    # 256307
    
    # Task 4b: Find total check-ins
    #check_ins = foursquare_data.count()
    time5 = datetime.now()
    #print "\nFind total check-ins", time5-time4, "\n", check_ins, "\n"
    # 19265256
    
    # Task 4c: Find total check-in sessions
    key_value_sessions = foursquare_data.map(lambda x: (x[2], 1))
    sessions = key_value_sessions.reduceByKey(add)
    sessions_count = sessions.count()
    time6 = datetime.now()
    print "\nFind total check-in sessions", time6-time5, "\n", sessions_count, "\n"
    # 6338302

    """
    The next two problems are difficult.
    It might be because the mapping is slow (assign_location()).
    Or it might be the count() in itself that is slow.
   "i want the proper numbers, ikke approx" 
    We could try to count approximately (there are ways).
    """
    
    # Task 4d: Find coutries represented
    key_value = foursquare_data_locations.map(lambda x: (x[9], 1))
    countries = key_value.reduceByKey(add)
    countries_count = countries.count()
    time7 = datetime.now()
    print "\nFind countries represented", time7-time6, "\n", countries_count, "\n"
    # benchmarked: @ 4min for 1/58th; @ 10min for 1/58th;  @ 5hours 4min for 58/58
    # results: 77; 10; 26
    
    # Task 4e: Find cities represented
    # "There could be two cities with the same name in the same/a different country"
    #key_value = foursquare_data_locations.map(lambda x: (x[10], 1))
    #cities = key_value.reduceByKey(add)
    #cities_count = cities.count()
    time8 = datetime.now()
    #print "\nFind cities represented", time8-time7, "\n", cities_count, "\n"
    # benchmarked: @ 4min for 1/58th; @ ?min for 58/58
    # results: 47, 413
    
    # Task 5: Creating histogram
    filter_sessions = sessions.filter(lambda x: x[1]>4)
    filter_sessions_tot = filter_sessions.map(lambda x: (x[1], 1))
    filter_sessions_tot = filter_sessions_tot.reduceByKey(add)
    take_100 = filter_sessions_tot.take(100)
    print take_100
    time9 = datetime.now()
    print "\nCreating histogram", time9-time8, "\n"
    
    # Task 6: 
    # (session_id, numberoftimes)
    #filter_sessions_col = filter_sessions.collect()
    #for i in range(10):
    #    print filter_sessions_col[i]
    #print len(filter_sessions_col)

    # full forsquare with numberoftimes > 4
    #distance_foursquare = foursquare_data_locations.filter(find_sessions)
    #take_100 = distance_foursquare.count()
    #print take_100
    
    #for i in range(10):
    #    print filter_sessions_col[i]
    #print len(filter_sessions_col)

    #distance_foursquare_col = distance_foursquare.collect()

    #distance_sessions = filter_sessions.map(set_distance)

    #filter_sessions = filter_sessions.map
    #filter_sessions = filter_sessions.map(lambda x: (x[0], 

    
    #distance_sessions = distance_sessions.map(set_distance)

    sc.stop()

def set_distance(data):
    time_list = {}
    for element in distance_foursquare_col:
        if element[2] == data[0]:
            time_list[element[3]] = (element[5], element[6])
    time_list = sorted(time_list, key=time_list.get)
    print time_list
    return 

def find_sessions(data):
    global filter_sessions_col
    for index in range(len(filter_sessions_col)):
        if data[2] == filter_sessions_col[index][0]:
            filter_sessions_col[index] = [data[2], data[3], data[5], data[6]]
            return True
    return False
    
def spark_init():
    conf = (SparkConf()
             .setMaster("local[2]")
             .setAppName("Foursquare")
             .set("spark.executor.memory", "4g"))
    sc = SparkContext(conf = conf)

    # Print some SparkConf variables
    print "\nSparkConf variables: ", conf.toDebugString()
    print "\nSparkConf id: ", sc.applicationId
    print "\nUser: ", sc.sparkUser()
    print "\nVersion: ", sc.version
    return sc

def c_init(data):
    line = data.split("\t")
    line[1] = float(line[1])
    line[2] = float(line[2])
    line.pop()
    line.pop() 
    return line
    
def f_init(data):
    line = data.split("\t")
    line[0] = int(line[0])
    line[1] = int(line[1])
    line[3] = datetime.strptime(line[3], "%Y-%m-%d %H:%M:%S")
    line[4] = int(line[4])
    line[5] = float(line[5])
    line[6] = float(line[6])
    return line

def generate_tsv(data):
    return "\t".join(data)

def set_time(data):
    new_date = datetime.strftime(data[3] + timedelta(minutes=data[4]),
                "%Y-%m-%d %H:%M:%S")
    data[3] = new_date
    data[4] = "0"
    return data

def assign_location(foursquare_data):
    distance = sys.maxint
    city = None
    country = None
    for city_line in cities_collection:
        temp_distance = haversine(foursquare_data[5], foursquare_data[6], 
                                    city_line[1], city_line[2])
        if (temp_distance < distance):
            distance = temp_distance
            city = city_line[0]
            country = city_line[3]
    foursquare_data.append(country)
    foursquare_data.append(city)
    return foursquare_data
    
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

if __name__ == "__main__":
    print("BIG DATA: FOURSQUARE\n")
    main()
