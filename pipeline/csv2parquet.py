from __future__ import print_function


import re
import os
import sys

from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


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
            StructField("genotype", ByteType(), True),
            StructField("samtools_raw_coverage", IntegerType(), True),
            StructField("gatk_filtered_coverage", IntegerType(), True),
            StructField("reads_ref", ShortType(), True),
            StructField("reads_alt", ShortType(), True),
            StructField("vqslod", FloatType(), True),
            StructField("genotype_qual_GQ", FloatType(), True),
            StructField("strand_bias_FS", FloatType(), True),
            StructField("haplotype_score", FloatType(), True),
            StructField("rms_map_qual_MQ", FloatType(), True),
            StructField("qual_by_depth_QD", FloatType(), True),
            StructField("qual", FloatType(), True),
            StructField("read_pos_rank_sum", FloatType(), True),
            StructField("map_qual_rank_sum", FloatType(), True),
            StructField("pass_fail_status", StringType(), True)
        ]
    elif schemaName == "ReadCoverage":
        # read coverage schema
        fields = [
            StructField("block_id", StringType(), True),
            StructField("sample_id", IntegerType(), True),
            StructField("min_coverage", StringType(), True)
        ]

    schema = StructType(fields)

    linesRDD = sqlContext.read.csv(
        inputFilePath, schema, nullValue='\\N')

    linesRDD.printSchema()

    linesRDD.write.parquet(outputDir)

    sc.stop()

if __name__ == '__main__':
    main()
