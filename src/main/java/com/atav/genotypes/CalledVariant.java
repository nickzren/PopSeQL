/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

import com.atav.genotypes.beans.Variant;
import com.atav.genotypes.conf.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
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
public class CalledVariant {

    //Columns
    /**
     *
     *  +------------------------+
        | COLUMN_NAME            |
        +------------------------+
        | 0.block_id               |
        | 1. sample_id              |
        | 2 .variant_id             |
        | 3 .chr     str               |
        | 4 .pos                    |
        | 5 .ref str               |
        | alt                    |
        | genotype               |
        | samtools_raw_coverage  |
        | gatk_filtered_coverage |
        | reads_ref              |
        | reads_alt              |
        | vqslod                 |
        | genotype_qual_GQ       |
        | strand_bias_FS         |
        | haplotype_score        |
        | rms_map_qual_MQ        |
        | qual_by_depth_QD       |
        | qual                   |
        | read_pos_rank_sum      |
        | map_qual_rank_sum      |
        | culprit                |
        | pass_fail_status       |
        +------------------------+
     */
//    private Dataset<Row> cvDF;
//    private JavaRDD<Row> cvRDD;
//    private JavaPairRDD<String, Row> cvPRDD;
//    private JavaPairRDD<String, Map<String,Variant>> transCVPRDD;
    
    
    //File loc :- /Users/kaustubh/Desktop/all_samples_called_variant.txt

    private JavaPairRDD<String, Map<String, Variant>> groupedCvPRDD;
    
    Map<String, String> options;
    private String cvQuery = "Select * from " + Configuration.schema + ".called_variant";
    SparkSession spsn;

    public CalledVariant(SparkSession sesh) {
        spsn = sesh;
        options = new HashMap<>();
        
        options.put("url", Configuration.url);
        options.put("dbtable", cvQuery); //default
        options.put("driver", Configuration.driver);
    }

//    public Dataset<Row> getCvDF() {
//        return cvDF;
//    }
//
//    public void setCvDF(Dataset<Row> cvDF) {
//        this.cvDF = cvDF;
//    }
//
//    public JavaPairRDD<String, Row> getCvPRDD() {
//        return cvPRDD;
//    }
//
//    public void setCvPRDD(JavaPairRDD<String, Row> cvPRDD) {
//        this.cvPRDD = cvPRDD;
//    }

    public JavaPairRDD<String, Map<String, Variant>> getGroupedCvPRDD() {
        return groupedCvPRDD;
    }

    public void setGroupedCvPRDD(JavaPairRDD<String, Map<String, Variant>> groupedCvPRDD) {
        this.groupedCvPRDD = groupedCvPRDD;
    }

    
    public void doFilter(String args) {
        
        cvQuery = cvQuery + args;
        cvQuery = "("+ cvQuery +") as t";                
        options.remove("dbtable");
        options.put("dbtable", cvQuery);
    }

//    public void setcvDF() {
//
//        cvDF = spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load()
//                .coalesce(2048);
//        
//        //System.out.println("Count : ...... " + cvDF.collectAsList().size());
//    }

//    public void setcvRDD() {
//        if (cvDF == null) {
//            setcvDF();
//        }        
//        cvRDD = spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load()
//                .coalesce(2048)
//                .toJavaRDD();                
//        //System.out.println("Count : ...... " + cvRDD.count());
//    }
    
//    public void setcvPRDD() {
//        if (cvRDD == null) {
//            setcvRDD();
//        }
//        cvPRDD = spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load()
//                .coalesce(2048)
//                .toJavaRDD()
//                .mapToPair((Row r) -> {
//                                return new Tuple2<String, Row>(r.getString(0), r);
//                            } );
//        transCVPRDD = spsn
//                        .read()
//                        .format("jdbc")
//                        .options(options)
//                        .load()
//                        .coalesce(2048)
//                        .toJavaRDD()
//                        .mapToPair((Row r) -> {
//                            return new Tuple2<String, Row>(r.getString(0), r);
//                        })
//                        .mapToPair(
//                                (Tuple2<String, Row> t1) -> {
//                                    Map<String, Variant> m = new HashMap<>();
//                                    m.put(Integer.toString(t1._2.getInt(2)), new Variant(t1._2)); //Map of Variant_Id and Variant Object
//                                    return new Tuple2<String, Map<String, Variant>>(t1._1, m);
//                                });
//        
//    }
    
     
    public CalledVariant doGrouping(String lim) {
        doFilter(lim);
        doGrouping();
        return this;
    }
    
    public void doGrouping() {
//        if (cvPRDD == null) {
//            setcvPRDD();
//        }        



        /***
         *  Downloading the file to Spark nodes
         * 
         */
        
        spsn
        .sparkContext()
        .addFile(Configuration.cvFile+"part-r-00000-ee630a23-c7ae-4676-88f5-947a7991288a.parquet");
        
        
        spsn
        .sparkContext()
        .addFile(Configuration.cvFile+"part-r-00001-ee630a23-c7ae-4676-88f5-947a7991288a.parquet");
        
                
        spsn
        .sparkContext()
        .addFile(Configuration.cvFile+"part-r-00002-ee630a23-c7ae-4676-88f5-947a7991288a.parquet");
        
        /**
         * mysql
         */
//                Dataset<Row> sourceToRDD=spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load();
 /**
  *             TEXT FILE
  */
//                Dataset<Row> sourceToRDD=spsn
//                .read()
//                .schema(getSchema())                        
//                .format("com.databricks.spark.csv")
//                .option("inferSchema", "false")
//                .option("nullValue", "\\N")
//                .option("header", "false")
//                .option("sep", "\t")
//                .csv(Configuration.cvFile);

/**
 *              PARQUET FILE
 * 
 */
            Dataset<Row> sourceToRDD=spsn
                                     .read()
                                     .parquet(SparkFiles.get("part*"))
                    ;


        groupedCvPRDD = sourceToRDD
                .repartition(300)
                .toJavaRDD()
                .mapToPair((Row r) -> {
                    return new Tuple2<String, Row>(r.getString(0), r);
                })
                .mapToPair(
                        (Tuple2<String, Row> t1) -> {
                            Map<String, Variant> m = new HashMap<>();
                            m.put(Integer.toString(t1._2.getInt(2)), new Variant(t1._2)); //Map of Variant_Id and Variant Object
                            return new Tuple2<String, Map<String, Variant>>(t1._1, m);
                        })
                .reduceByKey((Map<String, Variant> t1, Map<String, Variant> t2) -> {
                    Map<String, Variant> r = new HashMap<>();
                    r.putAll(t1);
                    t2.keySet().stream().forEach((s) -> {
                        if (r.containsKey(s)) {
                            r.get(s).getCarrierMap().putAll(t2.get(s).getCarrierMap());
                        } else {
                            r.put(s, t2.get(s));
                        }
                    });
                    t1.clear();
                    t2.clear();
                    return r;
                });
        
    }
    
    
        public static StructType getSchema() {
        StructField[] fields = {
            DataTypes.createStructField("Block ID", DataTypes.StringType, true),
            DataTypes.createStructField("Sample ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("Variant ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("Chr", DataTypes.StringType, true),
            DataTypes.createStructField("Pos", DataTypes.IntegerType, true),
            DataTypes.createStructField("Ref", DataTypes.StringType, true),
            DataTypes.createStructField("Alt", DataTypes.StringType, true),
            DataTypes.createStructField("genotype", DataTypes.IntegerType, true),
            DataTypes.createStructField("samtools", DataTypes.IntegerType, true),
            DataTypes.createStructField("gatk", DataTypes.IntegerType, true),
            DataTypes.createStructField("readsref", DataTypes.IntegerType, true),
            DataTypes.createStructField("readsalt", DataTypes.IntegerType, true),
            DataTypes.createStructField("vqslod", DataTypes.FloatType, true),
            DataTypes.createStructField("Geno Qual", DataTypes.DoubleType, true),
            DataTypes.createStructField("Strand Bias FS", DataTypes.DoubleType, true),
            DataTypes.createStructField("Haplo Score", DataTypes.DoubleType, true),
            DataTypes.createStructField("RMS Map Qual MQ", DataTypes.DoubleType, true),
            DataTypes.createStructField("Qual by depth QD", DataTypes.DoubleType, true),
            DataTypes.createStructField("Qual", DataTypes.createDecimalType(7,2), true),
            DataTypes.createStructField("REad pos rank sum", DataTypes.DoubleType, true),
            DataTypes.createStructField("Map qual rank sum", DataTypes.DoubleType, true),
            DataTypes.createStructField("Culprit", DataTypes.StringType, true),
            DataTypes.createStructField("Pass Fail", DataTypes.StringType, true)
        };
        return new StructType(fields);
    }
}
