from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext


def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0.10 and float(p[11])> 0.10 and float(p[16])> 0.10):
                return p


def main():
	#Remove invalid lines
	sc = SparkContext(appName="Active_Taxis")
	lines = sc.textFile(sys.argv[1],1)
	lines = lines.map(lambda x: x.split(',')).filter(lambda y: correctRows(y))

	#Task 1
	#Get taxi id and driver's id, then group by taxi id and remove duplicates.
	lines1 = lines.map(lambda x: [x[0],x[1]]).groupByKey().mapValues(set)
	#Number of unique drivers for each taxi.
	lines1 = lines1.mapValues(len)
	#Sort by largest number of drivers and get the top 10 taxi id.
	lines1 = lines1.map(lambda x: [x[1],x[0]]).sortByKey(False).map(lambda y: y[1]).take(10)
	result1 = sc.parallelize(lines1).coalesce(1)
	result1.saveAsTextFile(sys.argv[2])
	sc.stop()



if __name__ == "__main__":
	main()
