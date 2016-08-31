/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

/**
 *
 * @author kaustubh
 */


import com.atav.genotypes.conf.Configuration;
import com.atav.genotypes.utils.SampleManager;
import com.atav.genotypes.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;


public class Generate {
    //Init session
            
public static SparkSession spsn = SparkSession
  .builder()
  //.config("spark.some.config.option", "some-value")
  .master(Configuration.master)
  .config("spark.shuffle.spill.compress","false")
  .appName("Genotype generator")
  .getOrCreate();
public static String limiter=" where 1=1";
//public static JavaPairRDD<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>> joinRes;



public static void main(String args[]){
    //Get Samples first
    SampleManager samp= new SampleManager(spsn);
    samp.doFiltering(limiter);
    samp.setSampleIds();
    
    
    System.out.println("Started grouping...");   
    
//    CalledVariant cv = new CalledVariant(spsn);
//            cv.doFilter(limiter);
//            cv.setcvPRDD();
    CalledVariant cv = (new CalledVariant(spsn)).doGrouping(limiter);
    ReadCoverage rc = (new ReadCoverage(spsn)).doGrouping(limiter);    
    Utils u = new Utils(samp.getBroadCastPheno(), samp.getBroadCastSamples());
    
            JavaPairRDD
                    .toRDD(cv
                            .getGroupedCvPRDD()
                            .join(rc
                                    .getGroupedRCPRDD()
                            )
                    ).toJavaRDD()
                     .map(u.joinMapper)
                     .map(u.outputMapper)
                     .flatMap(u.elementsOfList)
                     .repartition(10)
                     .saveAsTextFile(Configuration.csvFilePath);
    
    spsn.stop();
}

}
