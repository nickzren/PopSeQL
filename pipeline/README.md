# PopSeQL Pipeline


## Usage
```
Usage: spark-submit (...) main.py
	 --in <annodb-dir>
		# Directory containing input files
	 --out <newdb-dir>
		# Directory to output new files
	[--variant-data-dir <var-data-dir>]
		# Directory containing all_snv.txt and all_indel.txt files
	[--local-files]
		# Force to use local filesystem instead of HDFS
	[--single-output]
		# Gather all sample data together
	[--samples <sample-names>]
		# Only read samples in the list (comma-separated sample names)
	[--emrfs]
		# Use EMRFS protocol instead of S3A
	[--aws-access-key <key>]
	[--aws-secret-key <key>]
```

**Note:** If using the ```--single-output``` option on Spark 1.6 or older, you must provide the Spark option <br /> ```--packages com.databricks:spark-csv_2.10:1.4.0``` (before the .py file).

## I/O Locations

When specifying a path for ```--in```, ```--out``` or ```--variant-data-dir```, the user might specify the location using one of the following prefixes:
* ```file://```, for local files (this path must be available to all Spark nodes);
* ```hdfs://```, for HDFS files;
* ```s3://```, for AWS S3 files;

If no prefix is given, Spark will use the default HDFS location, if Hadoop is configured (Spark searches for the ```$HADOOP_CONF_DIR``` directory), or the local filesystem, otherwise. The user might force the application to use the local filesystem by using the ```--local-files``` option.

**Note:** When using S3, the AWS CLI must be installed and the user must provide the AWS S3A Hadoop libraries (```aws-java-sdk-X.X.X.jar``` and ```hadoop-aws-X.X.X.jar```) to Spark using the Spark option ```--jars```. Such libraries are located under ```$HADOOP_HOME/share/hadoop/tools/lib```. However, if the application is being run on a AWS Elastic MapReduce (EMR) cluster, which has native support to S3 (using EMRFS), then such libraries are not necessary and the option ```--emrfs``` must be used.

## Input files

The pipeline expects to receive as input (```--in``` option) the path to a folder in which every sample has its own subfolder. For a given sample \<SAMPNAME\>, it expects a subfolder ```<SAMPNAME>/``` containing:
* ```<SAMPNAME>/Called_variation/<SAMPNAME>_called_snv.txt```, which has the called SNV records;
* ```<SAMPNAME>/Called_variation/<SAMPNAME>_called_indel.txt```, which has the called INDEL records;
* ```<SAMPNAME>/<SAMPNAME>_read_coverage_1024_chr<CHR>.txt```, containing read coverage data for each chromosome ```<CHR>```.

The input path must contain the files ```all_snv.txt``` and ```all_indel.txt``` or the path to the folder containing such files must be provided using the ```--variant-data-dir``` option.

## Output files

Regardless of the filesystem location (local or HDFS), the output depends mostly on the ```--single-output``` option. To output all data together (e.g. to load on database), pick the ```--single-output``` option, which is considerably faster than outputting each sample on a different folder.

* If such option is **not** provided, then, for each sample \<SAMPNAME\>, there will be an output folder ```<SAMPNAME>/``` under the output directory (and under the same relative path as the input folder - see Note 2) containing:
	* ```<SAMPNAME>/<SAMPNAME>_called_variation.txt```, which has all the called variation data;
	* ```<SAMPNAME>/<SAMPNAME>_read_coverage_1024.txt```, which has all the read coverage data.

* Otherwise, all sample data will be merged, and the output folder will contain:
	* ```all_samples/all_samples_called_variation.txt```, which has all the called variation data;
	* ```all_samples/all_samples_read_coverage_1024.txt```, which has all the read coverage data.

**Note 1:** The sample subfolders don't need to be immediatly under the input folder, e.g.:
For a input path ```/path/to/input```, there might be a sample folder ```/path/to/input/sample1``` as well as another one ```/path/to/input/a/b/c/sample2```. In this case, if ```--single-output``` is not provided, the output sample folders will be ```/path/to/output/sample1``` and ```/path/to/output/a/b/c/sample2```

**Note 2:** For HDFS, instead of ```.txt``` files, the output will be stored in folders ended in ```_hdfs```.
