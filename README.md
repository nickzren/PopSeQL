# PopSeQL

PopSeQL currently implements ATAV's List Variant Genotype function using Apache Spark for MPP.

For now, it reads parquet files containing denormalized called_variant and read_coverage data. The path to such files is defined in ```function.genotype.base.CalledVariantSparkUtils```. When changing the paths, remember to double check the location (i.e. local FS, HDFS etc.)

PopSeQL requires the user to define the samples file using ```--sample```, which contains one sample_id and pheno (0 for ctrl or 1 for case) delimited by an empty space per line. The output path must also be defined using ```--out```.

It supports all Genotype Level Filter Options (which may be found [here](https://redmine.igm.cumc.columbia.edu/projects/atav/wiki/Genotype_Level_Filter_Options)).

## Running on EMR

To start a EMR cluster, go to the EMR console, "Create cluster", "Go to advanced options".

Leave only Hadoop and Spark checked (make sure the Spark version is at least 2.0). Check "Load JSON from S3" and provide the path ```s3://popseql/emr-config.json```. Click "Next".

Choose the instance types you want to use, then click "Next".

Click "Next" on the following screen.

On the last screen, pick the key pair you want to use and make sure to use a security group that allows you to access the cluster. Then click "Create cluster".

After the clsuter starts, you can ssh to it or create steps using the EMR console.

To copy data between S3 and HDFS, use ```s3-dist-cp``` (which can be added as a step).


