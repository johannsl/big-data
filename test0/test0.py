from datetime import datetime
from datetime import timedelta
from math import *
from operator import add
from operator import itemgetter
from pyspark import SparkConf
from pyspark import SparkContext
import sys

def main():
    sc = spark_init()
    a = sc.emptyRDD()
    print "\nemptyrdd", a
    b = sc.parallelize(xrange(0, 10), 5).glom().collect()
    print "\nparallelize", b
    



    sc.stop()
    
def spark_init():
    conf = (SparkConf()
             .setMaster("local")
             .setAppName("TEST0")
             .set("spark.executor.memory", "2g"))
    sc = SparkContext(conf = conf)
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

if __name__ == "__main__":
    print("BIG DATA: TEST0\n")
    main()
