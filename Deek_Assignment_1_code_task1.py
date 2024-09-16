from __future__ import print_function

import os
import sys
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Function - Cleaning
# For example, remove lines if they don’t have 17 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]):
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return p

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output1>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])
    
    cleaned_rdd = rdd.map(lambda line: line.split(",")).filter(lambda line: correctRows(line))

    # Task 1: Top-10 Active Taxis -- map each row to (taxi_id, driver_id), removes duplicates, grouping this by taxi_id, counting distinct drivers
    top10uniqueactivetaxis = cleaned_rdd.map(lambda p: (p[0], p[1])) \
                                  .distinct() \
                                  .groupByKey() \
                                  .mapValues(lambda x: len(x)) \
                                  .top(10, key=lambda x: x[1])

    # Save Task 1 output to argument
    results_1 = sc.parallelize(top10uniqueactivetaxis).coalesce(1)
    results_1.saveAsTextFile(sys.argv[2])

    sc.stop()