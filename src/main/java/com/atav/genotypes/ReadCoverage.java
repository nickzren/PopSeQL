/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

import static com.atav.genotypes.CalledVariant.getSchema;
import com.atav.genotypes.conf.Configuration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;


/**
 *
 * @author kaustubh
 */
public class ReadCoverage {
    //Columns
    
    /***
     * 
     *  +--------------+----------------+------+-----+---------+-------+
        | Field        | Type           | Null | Key | Default | Extra |
        +--------------+----------------+------+-----+---------+-------+
        | block_id     | varchar(11)    | NO   | PRI | NULL    |       |
        | sample_id    | int(11)        | NO   | PRI | NULL    |       |
        | min_coverage | varchar(20000) | NO   |     | NULL    |       |
        +--------------+----------------+------+-----+---------+-------+
     * 
     * @param <error>
     */
    private Map<String, String> options;
    private String cvQuery = "Select * from " + Configuration.schema + ".read_coverage";
    private SparkSession spsn;
    //Read Coverage location:- /Users/kaustubh/Desktop/all_samples_read_coverage_1024.txt
//    private Dataset<Row> rcDF;
//    private JavaRDD<Row> rcRDD;
//    private JavaPairRDD<String, Row> rcPRDD;
//    private JavaPairRDD<String, Map<String,TreeMap<Integer,String>>> transRCPRDD;
    private JavaPairRDD<String, Map<String, TreeMap<Integer,String>>> groupedRCPRDD;
    
    public ReadCoverage(SparkSession sesh){
        spsn = sesh;
        options = new HashMap<>();
        options.put("url", Configuration.url);
        options.put("dbtable", cvQuery); //default
        options.put("driver", Configuration.driver);
    }
    
    
    
    public void doFilter(String args) {        
        cvQuery = cvQuery + args;
        cvQuery = "("+ cvQuery +") as t";                
        options.remove("dbtable");
        options.put("dbtable", cvQuery);
    }

//    public Dataset<Row> getRcDF() {
//        return rcDF;
//    }

//    public void setRcDF() {
//        rcDF = spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load()
//                .coalesce(2048);
//    }
    
//    public void setrcRDD(){
//     if (rcDF==null){
//         setRcDF();
//     }
//     rcRDD=spsn
//             .read()
//             .format("jdbc")
//             .options(options)
//             .load()
//             .coalesce(2048)
//             .toJavaRDD();
//    }
    
//    public void setrcPRDD(){
//        if(rcRDD==null){
//            setrcRDD();
//        }
//        rcPRDD=spsn
//             .read()
//             .format("jdbc")
//             .options(options)
//             .load()
//             .coalesce(2048)
//             .toJavaRDD()
//             .mapToPair((Row r) -> {
//            return new Tuple2<String, Row>(r.getString(0), r); // BlockID and Row
//        } );
//        
//        transRCPRDD = spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load()
//                .coalesce(2048)
//                .toJavaRDD()
//                .mapToPair((Row r) -> {
//                    return new Tuple2<String, Row>(r.getString(0), r); // BlockID and Row
//                })
//                .mapToPair((Tuple2<String, Row> t1) -> {
//                    Map<String, TreeMap<Integer, String>> m = new HashMap<>();
//                    boolean isNum = true;
//                    int covKey = 0;
//                    TreeMap<Integer, String> res = new TreeMap<>();
//                    String[] rcVals = t1._2.getString(2).trim()
//                            .split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)"); //Split Coverage string
//                    //Coverage range goes first, Val later
//                    for (String cov : rcVals) {
//                        if (isNum) {
//                            covKey += new Integer(cov);
//                            isNum = false;
//                        } else {
//                            res.put(covKey, cov);
//                            isNum = true;
//                        }
//                    }
//                    m.put(Integer.toString(t1._2.getInt(1)), //Sample ID
//                            res); //Coverage TreeMap
//                    return new Tuple2<String, Map<String, TreeMap<Integer, String>>>(t1._1, m); //Block ID and Sample+Coverage Map
//                });
//    }
    public ReadCoverage doGrouping(String lim){
        doFilter(lim);
        doGrouping();
        return this;
    }
    public void doGrouping(){
//        if(rcPRDD==null){
//            setrcPRDD();
//        }
        /**
         * Downloading readCoverage file at all nodes
         * 
         */
        
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00000-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
        
        
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00001-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
        
                
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00002-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");


        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00003-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
                
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00004-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
                        
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00005-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
                                
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00006-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");        
                                        
                                        
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00007-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
        
        
        spsn
        .sparkContext()
        .addFile("/Users/kaustubh/Desktop/parquet/read_coverage/part-r-00008-93ad10f4-ec5b-411d-9789-20921a8e2b9e.parquet");
        /**
         * 
         * spsn
         * .read() // DataFRameReader
         * .
         * 
         */
        
        
        
        /**
         * switch sources here
         */
//        Dataset<Row> sourceToRDD=spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load();

//      Dataset<Row> sourceToRDD=spsn
//                .read()
//                .schema(getSchema())                        
//                .format("com.databricks.spark.csv")
//                .option("nullValue", "\\N")
//                .option("inferSchema", "false")
//                .option("header", "false")
//                .option("sep", "\t")
//                .csv(Configuration.rcFile);
      
        /***               PARQUET         */
        
            Dataset<Row> sourceToRDD=spsn
                                     .read()
                                     .parquet(SparkFiles.get("part*"));
        
        groupedRCPRDD = sourceToRDD
                .coalesce(2048)
                .toJavaRDD()
                .mapToPair((Row r) -> {
                    return new Tuple2<String, Row>(r.getString(0), r); // BlockID and Row
                })
                .mapToPair((Tuple2<String, Row> t1) -> {
                    Map<String, TreeMap<Integer, String>> m = new HashMap<>();
                    boolean isNum = true;
                    int covKey = 0;
                    TreeMap<Integer, String> res = new TreeMap<>();

                    
                 if(null!=t1._2.getString(2)){   
                    String[] rcVals = t1._2.getString(2).trim()
                                .split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)"); //Split Coverage string
                        //Coverage range goes first, Val later
                        for (String cov : rcVals) {
                            if (isNum) {
                                covKey += new Integer(cov);
                                isNum = false;
                            } else {
                                res.put(covKey, cov);
                                isNum = true;
                            }
                        }
                 }else {
                     res.put(1024, "a");
                 }
                    m.put(Integer.toString(t1._2.getInt(1)), //Sample ID
                                                        res); //Coverage TreeMap
                
                    
                    return new Tuple2<String, Map<String, TreeMap<Integer, String>>>(t1._1, m); //Block ID and Sample+Coverage Map
                })
                .reduceByKey((Map<String, TreeMap<Integer, String>> t1, Map<String, TreeMap<Integer, String>> t2) -> {
                    Map<String, TreeMap<Integer, String>> r = new HashMap<>();
                    r.putAll(t1);
                    r.putAll(t2);
                    return r;
                });
    }

    public JavaPairRDD<String, Map<String, TreeMap<Integer, String>>> getGroupedRCPRDD() {
        return groupedRCPRDD;
    }
    
    
        public static StructType getSchema() {
         StructField[] fields = {
            DataTypes.createStructField("Block ID", DataTypes.StringType, true),
            DataTypes.createStructField("Sample ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("Coverage value", DataTypes.StringType, true)
            };
        return new StructType(fields);
        }
}
