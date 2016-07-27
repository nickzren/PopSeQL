import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.Properties;
import java.util.StringJoiner;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 *
 * @author nick
 */
public class Program {
    
    public static final int STEPSIZE = 100;
    
    public static JavaSparkContext sc;
    public static SQLContext sqlContext;
    public static String jdbcURL =
            "jdbc:mysql://localhost:3306/annodb?user=test&password=test";
    
    public static DataFrame getCalledVariantDF() {
        return
            sqlContext.read().jdbc(jdbcURL,
                    "( select * from called_variant where chr = '1' ) t1",
                    new Properties());
    }
    
    public static DataFrame getSampleIdDF( DataFrame cvDF ) {
        return cvDF.select("sample_id").distinct()
                .withColumnRenamed("sample_id", "p_sample_id");
    }
    
    public static DataFrame getChrPosDF( DataFrame cvDF ) {
        return cvDF.select("chr", "pos", "block_id").distinct()
                .withColumnRenamed("chr", "p_chr")
                .withColumnRenamed("pos", "p_pos")
                .withColumnRenamed("block_id", "p_block_id");
    }
    
    public static DataFrame getBlockIdDF( DataFrame cvDF ) {
        return cvDF.select("block_id").distinct();
    }
    
    public static DataFrame getCarrierDF( DataFrame posSampleCrossDF, DataFrame cvDF) {
        return posSampleCrossDF.join(cvDF,
                    cvDF.col("sample_id").equalTo(posSampleCrossDF.col("p_sample_id"))
                    .and(cvDF.col("chr").equalTo(posSampleCrossDF.col("p_chr")))
                    .and(cvDF.col("pos").equalTo(posSampleCrossDF.col("p_pos"))),
            "left");
    }

    public static void main(String[] args) {
            //String logFile = "/usr/local/Cellar/apache-spark/1.6.1/sparkTestLog.txt";       
    SparkConf conf = new SparkConf().setAppName("Example of Spark SQL Connection"); // Set this on commandline  .setMaster("spark://igm-it-spare.local:7077");
    sc = new JavaSparkContext(conf);
    
    sqlContext = new SQLContext(sc);
    
    System.out.println(">>> Loading called variant data");
    DataFrame cvDF = getCalledVariantDF()
            .persist(StorageLevel.MEMORY_AND_DISK_SER());
    
    DataFrame sampleIdDF = getSampleIdDF(cvDF);
    
    List<String> sampleIdsList = //(String [])
            sampleIdDF.toJavaRDD()
            .map((Row r) -> Integer.toString(r.getInt(0)))
            .collect();
    
    String[] sampleIds = sampleIdsList.toArray(new String[sampleIdsList.size()]);
    
    DataFrame posDF = getChrPosDF(cvDF);
    
//    System.out.println(sampleIdDF.count());
//    System.out.println(posDF.count());
    
    DataFrame posSampleCrossDF = 
            posDF.join(sampleIdDF, lit(true), "outer");
    
//    sampleIdDF.unpersist();
    
    System.out.println(">>> Generating carrier data");
    // carrierDF columns: |p_chr|p_pos|p_block_id|p_sample_id| <called_variant_columns>
    DataFrame carrierDF =
            getCarrierDF(posSampleCrossDF, cvDF)
            .persist(StorageLevel.MEMORY_AND_DISK_SER());
    
    DataFrame blockIdDF =
            getBlockIdDF(cvDF);
    
    List<String> blockIds =
            blockIdDF.toJavaRDD()
            .map((Row r) -> r.getString(0))
            .collect();
    
    cvDF.unpersist();
    
    for(int i=0;i<blockIds.size();i+=STEPSIZE) {
        int begin = i;
        int end = (i+STEPSIZE < blockIds.size()) ? i+STEPSIZE : blockIds.size();
        
        List<String> filteredBlockIdsList =
                blockIds.subList(begin, end);
        
        String[] filteredBlockIds =
            filteredBlockIdsList.toArray(new String[filteredBlockIdsList.size()]);
        
        String commaSepBlockIds =
                Utils.strjoin(filteredBlockIds,", ");
        
        System.out.println(">>> Processing blocks... ["+begin+","+end+"[");
        System.out.println(commaSepBlockIds);
        
        DataFrame filteredCarrierDF =
                carrierDF.where(carrierDF.col("p_block_id").in((Object[])filteredBlockIds));
        
        DataFrame readCoverageDF =
                sqlContext.read().jdbc(jdbcURL,
                    "( select * from read_coverage\n" +
                        "where block_id IN ( "+commaSepBlockIds+" )\n" +
                        "and sample_id IN ( "+Utils.strjoin(sampleIds,", ")+" ) ) t1",
                    new Properties());
        
        filteredCarrierDF.show();
        readCoverageDF.show();
        
        JavaPairRDD<String,Iterable<Row>> carrierBlockRDD = filteredCarrierDF.toJavaRDD()
                .mapToPair((Row r) ->
                    new Tuple2<>(
                        r.getString(2)+"-"+Integer.toString(r.getInt(3)), r
                    ))
                .groupByKey();
        
        JavaPairRDD<String,Row> readCoverageRDD = readCoverageDF.toJavaRDD()
                .mapToPair((Row r) ->
                    new Tuple2<>(
                        r.getString(0)+"-"+Integer.toString(r.getInt(1)), r
                    ));
        
        JavaRDD<Tuple2<Iterable<Row>, Row>> carrierCoveragePairsRDD =
                carrierBlockRDD.join(readCoverageRDD).values();
        
        // TODO: use .flatMap() to decompress coverage data and build dataset
        
        
//        System.exit(-1);
        System.out.println();
        System.out.println();
        
    }
    
    sc.stop();
   }
}
