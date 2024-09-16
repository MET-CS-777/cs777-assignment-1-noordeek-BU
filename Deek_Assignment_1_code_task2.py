from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

# Exception Handling and removing wrong data lines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Function - Cleaning
# Remove lines if they don’t have 17 values and validate specific columns
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

    # Task 2: Calculate the top-10 best drivers (15 points) based on earnings per minute
    top10bestdrivers = cleaned_rdd \
        .map(lambda x: (x[1], (float(x[16]) / (float(x[4]) / 60), 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1])) \
        .top(10, key=lambda x: x[1])

    # Save Task 2 output to arugment
    results_2 = sc.parallelize(top10bestdrivers).coalesce(1)
    results_2.saveAsTextFile(sys.argv[2])

    sc.stop()
