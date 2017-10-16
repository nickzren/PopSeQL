from __future__ import print_function

import re
import os
import sys

from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import glob


def main():
    print("\n --- CSV to Parquet Convertion ---\n")

    # command line parsing
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--in":
            inputFilePath = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--out":
            outputDir = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--schema-name":
            schemaName = sys.argv[i + 1]
            i = i + 1
        else:
            print("Invalid argument: " + sys.argv[i], file=sys.stderr)
            exit(-1)
        i = i + 1

    # spark process
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    if schemaName == "CalledVariant":
        # called variant schema
        fields = [
            StructField("block_id", StringType(), True),
            StructField("sample_id", IntegerType(), True),
            StructField("chr", StringType(), True),
            StructField("pos", IntegerType(), True),
            StructField("ref", StringType(), True),
            StructField("alt", StringType(), True),
            StructField("GT", ByteType(), True),
            StructField("DP", ShortType(), True),
            StructField("AD_REF", ShortType(), True),
            StructField("AD_ALT", ShortType(), True),
            StructField("GQ", IntegerType(), True),
            StructField("VQSLOD", FloatType(), True),
            StructField("FS", FloatType(), True),
            StructField("MQ", ShortType(), True),
            StructField("QD", ShortType(), True),
            StructField("QUAL", IntegerType(), True),
            StructField("ReadPosRankSum", FloatType(), True),
            StructField("MQRankSum", FloatType(), True),           
            StructField("FILTER", StringType(), True)
        ]
                       
    elif schemaName == "DP_bins":
        # read coverage schema
        fields = [
            StructField("block_id", StringType(), True),
            StructField("sample_id", IntegerType(), True),
            StructField("DP_string", StringType(), True)
        ]

    schema = StructType(fields)
    
    linesRDD = sqlContext.read.format('com.databricks.spark.csv')\
        .options(header='false',delimiter='\t',treatEmptyValuesAsNulls = 'true',nullValue='\\N').load(inputFilePath, schema = schema)

    linesRDD.printSchema()
    linesRDD.show()
    linesRDD.write.parquet(outputDir)
    
    sc.stop()

if __name__ == '__main__':
    main()