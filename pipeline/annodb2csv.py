from __future__ import print_function

import re
import os
import sys


from operator import add

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import *
from subprocess import *

awsAccessKey = None
awsSecretKey = None

s3Prefix = 's3a://'


def get_file_paths(directory, regex):
    file_paths = []

    for root, directories, files in os.walk(directory):
        for filename in files:
            match = re.search(regex, filename)
            if match:
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)

    return file_paths


def get_file_paths_hdfs(directory, regex):
    file_paths = []

    command = ['hadoop', 'fs', '-ls', '-R', directory]

    syscall = Popen(command, stdout=PIPE, stderr=DEVNULL)
    for line in syscall.stdout:
        filepath = str(line).rsplit(' ', 1)[-1].replace('\\n\'', '')
        match = re.search(regex, filepath)
        if match:
            file_paths.append(filepath)

    return file_paths


def get_file_paths_s3(directory, regex):
    file_paths = []

    if awsAccessKey == None or awsSecretKey == None:
        print("Cannot get data from S3 without credentials", file=sys.stderr)
        exit(-1)

    os.environ['AWS_ACCESS_KEY_ID'] = awsAccessKey
    os.environ['AWS_SECRET_ACCESS_KEY'] = awsSecretKey

    bucketName = directory.split('/')[2]
    global s3Prefix
    bucketURI = s3Prefix + bucketName

    # directory = directory.replace('s3a:','s3:')

    command = ['aws', 's3', 'ls', '--recursive', directory]

    syscall = Popen(command, stdout=PIPE)
    for line in syscall.stdout:
        filepath = str(line).rsplit(' ', 1)[-1].replace('\\n\'', '')
        match = re.search(regex, filepath)
        if match:
            file_paths.append(os.path.join(bucketURI, filepath))

    return file_paths


def write_file(outDir, prefix, rdd, fileSuffix, isHDFS):
    outputDir = os.path.join(outDir, prefix)
    filePlacePrefix = ''
    if not isHDFS:
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        filePlacePrefix = 'file://'
    rdd.coalesce(8, True).saveAsTextFile(filePlacePrefix +
                                         os.path.join(outputDir, (prefix.split('/')[-1]) + fileSuffix))


def main():
    print("\n --- PopSeQL Pipe ---\n")

    if sys.version_info < (3, 0):
        print("This application requires Python 3.\nExiting...", file=sys.stderr)
        exit(-1)

    inputDir = None
    outputDir = None
    varDataDir = None
    singleOutput = True
    sampleNames = None
    forceHDFS = True
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--in":
            inputDir = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--out":
            outputDir = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--variant-data-dir":
            varDataDir = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--single-output":
            singleOutput = True
        elif sys.argv[i] == "--samples":
            sampleNames = set(sys.argv[i + 1].split(","))
            i = i + 1
        elif sys.argv[i] == "--local-files":
            forceHDFS = False
        elif sys.argv[i] == "--aws-access-key":
            global awsAccessKey
            awsAccessKey = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--aws-secret-key":
            global awsSecretKey
            awsSecretKey = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == "--emrfs":
            global s3Prefix
            s3Prefix = 's3://'
        else:
            print("Invalid argument: " + sys.argv[i], file=sys.stderr)
            exit(-1)
        i = i + 1

    if varDataDir == None:
        varDataDir = inputDir

    if inputDir == None or outputDir == None:
        print("Usage: spark-submit (...) main.py" +
              "\n\t --in <annodb-dir>\n\t\t# Directory containing input files" +
              "\n\t --out <newdb-dir>\n\t\t# Directory to output new files" +
              "\n\t[--variant-data-dir <var-data-dir>]\n\t\t# Directory containing all_snv.txt and all_indel.txt files" +
              "\n\t[--local-files]\n\t\t# Force to use local filesystem instead of HDFS" +
              "\n\t[--single-output]\n\t\t# Gather all sample data together" +
              "\n\t[--samples <sample-names>]\n\t\t# Only read samples in the list (comma-separated sample names)"
              "\n\t[--emrfs]\n\t\t# Use EMRFS protocol instead of S3A" +
              "\n\t[--aws-access-key <key>]" +
              "\n\t[--aws-secret-key <key>]", file=sys.stderr)
        exit(-1)

    # Check if input files exist
    if (forceHDFS or "hdfs://" in inputDir or "s3://" in inputDir) and not "file://" in inputDir:
        print("Attention: input HDFS file checks to be implemented.\n\tMake sure the paths given are correct")
        absPathToInputFolder = inputDir
        if "s3://" in inputDir:
            inputFilePaths = get_file_paths_s3(
                inputDir, r'(read_coverage_1024_chr)|(called_)')
        else:
            inputFilePaths = get_file_paths_hdfs(
                inputDir, r'(read_coverage_1024_chr)|(called_)')
    else:
        if 'file://' in inputDir:
            if not os.path.exists(inputDir[7:]) or not os.access(inputDir[7:], os.R_OK):
                print("Error accessing input directory!", file=sys.stderr)
                exit(-1)
            # Get filepaths
            inputFilePaths = get_file_paths(
                inputDir[7:], r'(read_coverage_1024_chr)|(called_)')
        else:
            if not os.path.exists(inputDir) or not os.access(inputDir, os.R_OK):
                print("Error accessing input directory!", file=sys.stderr)
                exit(-1)
            inputFilePaths = get_file_paths(
                inputDir, r'(read_coverage_1024_chr)|(called_)')

        absPathToInputFolder = os.path.abspath(inputDir)

    if (forceHDFS or "hdfs://" in outputDir or "s3://" in outputDir) and not "file://" in outputDir:
        print("Attention: output HDFS file checks to be implemented.\n\tMake sure the paths given are correct")
    else:
        if 'file://' in outputDir:
            if not os.path.exists(outputDir[7:]) or not os.access(outputDir[7:], os.W_OK):
                print("Error accessing output directory!", file=sys.stderr)
                exit(-1)
        else:
            if not os.path.exists(outputDir) or not os.access(outputDir, os.W_OK):
                print("Error accessing output directory!", file=sys.stderr)
                exit(-1)

    if "s3://" in outputDir:
        outputDir = outputDir.replace('s3://', s3Prefix)

    if sampleNames != None:
        inputFilePaths = filter(lambda path: any(
            sampleName + "/" in path for sampleName in sampleNames), inputFilePaths)

    if "s3://" in varDataDir:
        varDataDir = varDataDir.replace('s3://', s3Prefix)

    # Check if variant data files exist
    snvDataPath = os.path.join(varDataDir, "all_snv.txt")
    indelDataPath = os.path.join(varDataDir, "all_indel.txt")

    if forceHDFS or "hdfs://" in varDataDir or "s3://" in varDataDir:
        print("Attention: variant data HDFS file checks to be implemented.\n\tMake sure the paths given are correct")
    else:
        if not 'file://' in varDataDir:
            snvDataPath = 'file://' + snvDataPath
            indelDataPath = 'file://' + indelDataPath
        # Check if all_snv.txt and all_indel.txt are valid
        if not os.path.isfile(snvDataPath[7:]) or not os.access(snvDataPath[7:], os.R_OK):
            print("Error accessing SNV data file!", file=sys.stderr)
            exit(-1)
        if not os.path.isfile(indelDataPath[7:]) or not os.access(indelDataPath[7:], os.R_OK):
            print("Error accessing INDEL data file!", file=sys.stderr)
            exit(-1)

    # Init Spark
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    if awsAccessKey != None:
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
        sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsAccessKey)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsSecretKey)
        sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsSecretKey)

    # Declare column names for:
    # - Variant data
    Var = Row("id", "chr", "pos", "ref", "alt")
    # - Called variante data
    CalledVar = Row("sample_id", "var_id", "genotype", "samtools_raw_coverage", "gatk_filtered_coverage",
                    "reads_ref", "reads_alt", "vqslod", "genotype_qual_GQ", "strand_bias_FS", "haplotype_score",
                    "rms_map_qual_MQ", "qual_by_depth_QD", "qual", "read_pos_rank_sum", "map_qual_rank_sum", "culprit", "pass_fail_status")

    print(snvDataPath)
    snvDF = sc.textFile(snvDataPath) \
        .map(lambda l: l.split('\t')) \
        .map(lambda l: Var(int(l[0]), l[1], int(l[2]), l[3], l[4])) \
        .toDF().cache()  # .persist(StorageLevel(True, True, False, False, 1))

    indelDF = sc.textFile(indelDataPath) \
        .map(lambda l: l.split('\t')) \
        .map(lambda l: Var(int(l[0]), l[1], int(l[2]), l[3], l[4])) \
        .toDF().cache()  # .persist(StorageLevel(True, True, False, False, 1))

    # Uncomment the lines below to read variant data from local database

    # snvDF = sqlContext.read.format('jdbc') \
    #     .options(url='jdbc:mysql://localhost', dbtable='pipe.snv', user='root', password='root') \
    #     .load().cache()
    # indelDF = sqlContext.read.format('jdbc') \
    #     .options(url='jdbc:mysql://localhost', dbtable='pipe.indel', user='root', password='root') \
    #     .load().cache()

    lastSample = None

    if singleOutput:

        #############
        # Output one folder for all samples
        #############
        chrDict = dict()
        calledSnvFiles = []
        calledIndelFiles = []
        sampleList = []

        for filepath in inputFilePaths:
            if (not forceHDFS and not 'file://' in filepath) or 'file://' in inputDir:
                filepath = 'file://' + filepath
            pathSplit = os.path.split(filepath)
            dirpath = pathSplit[0]  # full path to directory containing file
            filename = pathSplit[1]
            # Get sample identifier (dir/name) e.g. 'custom_capture/hh1234'
            sample = os.path.relpath(dirpath, absPathToInputFolder)

            # Eliminate "/Called_Variation" from sample identifier, if needed
            if "Called_Variation" in sample:
                sample = sample[:-17]

            if sample != lastSample:
                sampleList.append(sample.rsplit('/', 1)[-1])

            # If it is read_coverage file
            if "read_coverage_" in filename:
                # Get chromosome from filename
                chrm = filename.split('.')[-2].split('chr')[-1]

                # Add file to corresponding
                if chrm in chrDict:
                    chrDict[chrm].append(filepath)
                else:
                    chrDict[chrm] = [filepath]

            # If it is called_snv file
            elif "called_snv" in filename:
                calledSnvFiles.append(filepath)

            # If it is called_indel file
            elif "called_indel" in filename:
                calledIndelFiles.append(filepath)

            else:
                print(">>> Ignoring " + filename)

            lastSample = sample

        print(">>> Samples being parsed:\n" + (', '.join(sampleList)) + "\n")

        # Compute SNV data
        print(">>> Gathering SNV data...")
        calledSnvDF = sc.textFile( ','.join(calledSnvFiles) ) \
            .map(lambda l: l.split('\t') ) \
            .map(lambda l: CalledVar(l[0], int(l[1]), l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17]) ) \
            .toDF()

        joinedSnvDF = calledSnvDF.join(
            snvDF, snvDF.id == calledSnvDF.var_id, 'left')

        # Compute INDEL data
        print(">>> Gathering INDEL data...")
        calledIndelDF = sc.textFile( ','.join(calledIndelFiles) ) \
            .map(lambda l: l.split('\t') ) \
            .map(lambda l: CalledVar(l[0], int(l[1]), l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17]) ) \
            .toDF()

        joinedIndelDF = calledIndelDF.join(
            indelDF, indelDF.id == calledIndelDF.var_id, 'left')
        joinedIndelDF = joinedIndelDF.withColumn(
            'var_id', -1 * joinedIndelDF.var_id)

        # Function to create 'block_id' from chrm and pos
        def genBlockId(chr, pos):
            return chr + "-" + str(pos // 1024)
        # Register it as UDF to use on DataFrames
        gen_block_id = udf(genBlockId, StringType())

        # Merge snv and indel DFs into one DF
        joinedDF = joinedSnvDF.unionAll(joinedIndelDF)
        # Generate block_id field
        joinedDF = joinedDF.withColumn(
            'block_id', gen_block_id(joinedDF.chr, joinedDF.pos))

        # Compute read_coverage data
        # Schema for reading files
        readSchema = StructType([
            StructField('sample_id', StringType(), True),
            StructField('pos', IntegerType(), True),
            StructField('read_coverage', StringType(), True)])

        # Schema for final DF
        finalSchema = StructType([
            StructField('sample_id', StringType(), True),
            StructField('pos', IntegerType(), True),
            StructField('read_coverage', StringType(), True),
            StructField('block_id', StringType(), True)])

        # Init empty DF
        readCoverageDF = sqlContext.createDataFrame([], finalSchema)

        for chrm in chrDict:
            print(">>> Gathering read_coverage data for chr " + chrm + "...")

            def create_tuple_helper(chrm):
                return (lambda line: convertLine(chrm, line))

            # Load all files for a chrm into one DF
            if sc.version >= '2.0':
                df = sqlContext.read.csv(chrDict[chrm], readSchema, '\t')
            else:
                df = sqlContext.read.format('com.databricks.spark.csv') \
                    .schema(readSchema).options(delimiter='\t').load(','.join(chrDict[chrm]))
            # Generate block_id
            df = df.withColumn('block_id', gen_block_id(lit(chrm), df.pos))

            # Append to big DF
            readCoverageDF = readCoverageDF.unionAll(df)

        # Remove commas from read_coverage
        readCoverageDF = readCoverageDF.withColumn(
            'read_coverage', regexp_replace('read_coverage', ',', ''))

        # Write files
        lastSample = "all_samples"
        # Flush last sample
        if(lastSample != None):
            # base path for all files
            filenameBase = os.path.join(
                outputDir, lastSample + '/' + lastSample)

            # Write and merge read_coverage data
            print(">>> Generating and writing " +
                  lastSample + "_read_coverage_1024.csv...")

            hdfsFilepath = filenameBase + '_read_coverage_1024_hdfs'
            if not forceHDFS:
                hdfsFilepath = 'file://' + hdfsFilepath
            finalFilepath = filenameBase + '_read_coverage_1024.csv'
            if sc.version >= '2.0':
                readCoverageDF.select('block_id', 'sample_id', 'read_coverage') \
                    .coalesce(sc.defaultParallelism).write.csv(hdfsFilepath, None, None, ',', '')
            else:
                readCoverageDF.select('block_id', 'sample_id', 'read_coverage') \
                    .coalesce(sc.defaultParallelism) \
                    .write.format('com.databricks.spark.csv').options(quote=' ', delimiter=',') \
                    .save(hdfsFilepath)
            if not forceHDFS:
                print(">>> Merging Hadoop files...")
                Popen('hadoop fs -getmerge ' + hdfsFilepath + ' ' + finalFilepath +
                      ' && hadoop fs -rm -f -r ' + hdfsFilepath, shell=True)

            # Write and merge called_variante data
            print(">>> Generating and writing " +
                  lastSample + "_called_variant.csv...")

            hdfsFilepath = filenameBase + '_called_variant_hdfs'
            if not forceHDFS:
                hdfsFilepath = 'file://' + hdfsFilepath
            finalFilepath = filenameBase + '_called_variant.csv'
            if sc.version >= '2.0':
                joinedDF.select('block_id', 'sample_id', 'chr', 'pos', 'ref', 'alt', "genotype", "samtools_raw_coverage", "gatk_filtered_coverage",
                                "reads_ref", "reads_alt", "vqslod", "genotype_qual_GQ", "strand_bias_FS", "haplotype_score",
                                "rms_map_qual_MQ", "qual_by_depth_QD", "qual", "read_pos_rank_sum", "map_qual_rank_sum", "pass_fail_status") \
                    .coalesce(sc.defaultParallelism).write.csv(hdfsFilepath, None, None, None, '', '', False)
            else:
                joinedDF.select('block_id', 'sample_id', 'chr', 'pos', 'ref', 'alt', "genotype", "samtools_raw_coverage", "gatk_filtered_coverage",
                                "reads_ref", "reads_alt", "vqslod", "genotype_qual_GQ", "strand_bias_FS", "haplotype_score",
                                "rms_map_qual_MQ", "qual_by_depth_QD", "qual", "read_pos_rank_sum", "map_qual_rank_sum", "pass_fail_status") \
                    .coalesce(sc.defaultParallelism) \
                    .write.format('com.databricks.spark.csv').options(quote=' ', delimiter=',') \
                    .save(hdfsFilepath)
            if not forceHDFS:
                print(">>> Merging Hadoop files...")
                Popen('hadoop fs -getmerge ' + hdfsFilepath + ' ' + finalFilepath +
                      ' && hadoop fs -rm -f -r ' + hdfsFilepath, shell=True)

if __name__ == "__main__":
    main()
