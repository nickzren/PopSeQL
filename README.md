# PopSeQL

PopSeQL currently implements ATAV's List Variant Genotype function using Apache Spark for MPP.

For now, it reads parquet files containing denormalized called_variant and read_coverage data. The input path needs to be specified using ```--called-variant``` and ```--read-coverage```.

PopSeQL requires the user to define the samples file using ```--sample```, which contains one sample_id and pheno (0 for ctrl or 1 for case) delimited by an empty space per line. The output path must also be defined using ```--out```.

## Running on LOCAL

To run PopSeQL locally, you need to install Spark 2.0+ and Hadoop 2.7. Copy over called variant and read coverage parquet format data to your local directory as well as the sample file.

```
$SPARK_HOME/bin/spark-submit --class Program ~/github/PopSeQL/target/PopSeQL-1.0.jar --list-var-geno --sample ~/samples.txt --called-variant file:///parquet/called_variant/*.parquet --read-coverage file:///parquet/read_coverage/*.parquet --ctrl-maf 0.01 --out ~/out
```

## Running on AWS EMR

To start a EMR cluster, go to the EMR console, "Create cluster", "Go to advanced options".

Leave only Hadoop and Spark checked (make sure the Spark version is at least 2.0). Check "Load JSON from S3" and provide the path ```s3://popseql/src/emr-config.json```. Click "Next".

Choose the instance types you want to use, then click "Next". (Recommand to use Spot instance)

Click "Next" on the following screen.

On the last screen, pick the key pair you want to use and make sure to use a security group that allows you to access the cluster. Then click "Create cluster".

After the clsuter starts, you can ssh to it or create steps using the EMR console.

To copy data between S3 and HDFS, use ```s3-dist-cp``` (which can be added as a step).

**EMR Steps**

1. Copy jar
  ```
  aws s3 cp s3://popseql/src/PopSeQL-1.0.jar /home/hadoop/
  ```
 
2. Copy sample file
  ```
  aws s3 cp s3://popseql/input/igm_ctrl/samples.txt /home/hadoop/
  ```

3. Copy data in
  ```
  s3-dist-cp --src=s3://popseql/input/igm_ctrl/parquet/ --dest=hdfs:///parquet/
  ```  

4. Run PopSeQL - spark job
  ```
  spark-submit --deploy-mode client --master yarn --num-executors 2 --executor-cores 4 --executor-memory 10g --class Program /home/hadoop/PopSeQL-1.0.jar --list-var-geno --sample /home/hadoop/samples.txt --out /out --ctrl-maf 0.001 --called-variant /parquet/called_variant/*.parquet --read-coverage /parquet/read_coverage/*.parquet
  ```

5. Copy data out
  ```
  s3-dist-cp --src=hdfs:///out --dest=s3://popseql/output/igm_ctrl/
  ```




