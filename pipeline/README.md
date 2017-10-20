To create an EMR cluster, do the following:
Set the region to US East (N. Virginia) and the availability zone to us-east-1a

1) Go to AWS -> EMR -> Create cluster.
2) Click on Advanced Options
3) Select the tools required. For this application, select Hadoop 2.7.3, Spark 2.2.0 and Ganglia 3.7.2. The later allows the user to monitor node performance through a web interface.
4) In edit software settings, select 'Load JSON from S3' and select s3://popseql/src/emr-config.json
5) The Add the steps as mentioned below according to the application. Alternatively, the user can ssh to the master node of the cluster and enter the commands via the command line.
6) For every step added, Step type must be 'Custom JAR' and JAR location must be 'command-runner.jar'. Enter the commands mentioned below in the Arguments section and click on Add. Do this for all the required commands. Click Next.
7) Choose the number of nodes required. Recommended setting is to select 'Spot' instance. Click Next.
8) Select "Terminate at task completion" and click Next.
9) Click on the EC2 key pair you wish to use. Create one if you want to SSH to the master node of the cluster and you don't have a key pair yet.

How to create an EC2 key pair:
1) Go to AWS -> EC2 -> create key pair
2) type in you key pair name and press create. 

How to Ssh to master node of cluster:
1) Go to AWS -> EC2 -> running instances
2) Select the instance which says 'master node' in the description, click connect and follow the instructions provided by AWS.
 
Before running PopSeQL, the parquet files must be generated. The commands to do this are as follows:

1) .txt files to parquet files. The steps are as follows:
a) Copy the called variant .txt files from s3 to hdfs
s3-dist-cp --src=s3://waldb/10k_exome/called_variant/ --dest=hdfs:///waldb/called_variant

b) Copy the DP bin .txt files from s3 to hdfs
s3-dist-cp --src=s3://waldb/10k_exome/DP_bins/ --dest=hdfs:///waldb/DP_bins

c) Copy the script to convert .txt files of DP bins as well as called variants from s3 to hdfs
aws s3 cp s3://popseql/src/csv2parquet.py /home/hadoop/

d) Run the spark code to convert the txt files to parquet for the variants. (num-executors = number of slave nodes selected)
spark-submit --deploy-mode client --master yarn --num-executors 99 --executor-cores 4 --executor-memory 10g /home/hadoop/csv2parquet.py --in /waldb/called_variant/ --out /waldb/parquet/called_variant/ --schema-name CalledVariant

e) Run the spark code to convert the txt files to parquet for the DP bins. The only difference from d) is the modified schema-name.
spark-submit --deploy-mode client --master yarn --num-executors 99 --executor-cores 4 --executor-memory 10g /home/hadoop/csv2parquet.py --in /waldb/DP_bins/ --out /waldb/parquet/DP_bins/ --schema-name DP_bins

Now the data needs to be copied from hdfs to s3.

f) Copy called variant parquet files to s3
s3-dist-cp --src=hdfs:///waldb/parquet/called_variant --dest=s3://popseql/input/10k_exome/parquet/called_variant

g) Copy DP bins parquet files to S3.
s3-dist-cp --src=hdfs:///waldb/parquet/DP_bins --dest=s3://popseql/input/10k_exome/parquet/DP_bins
