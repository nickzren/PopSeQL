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


import com.atav.genotypes.beans.Variant;
import com.atav.genotypes.conf.Configuration;
import com.atav.genotypes.utils.SampleManager;
import com.atav.genotypes.utils.Utils;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


public class Generate {
    //Init session
            
public static SparkSession spsn = SparkSession
  .builder()
  //.config("spark.some.config.option", "some-value")
  .appName("Genotype generator")
  .master(Configuration.master)
  .getOrCreate();
public static String limiter=" where block_id IN (\"X-125694\",\"X-120643\",\"X-120619\",\"X-120080\", \"X-107153\") ";
public static JavaPairRDD<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>> joinRes;



public static void main(String args[]){
    //Get Samples first
    SampleManager samp= new SampleManager(spsn);
    samp.doFiltering(limiter);
    samp.setSampleIds();
    
    
    //Called Vars
    CalledVariant cv = new CalledVariant(spsn);
    //Read Cov
    ReadCoverage rc = new ReadCoverage(spsn);
    System.out.println("Started grouping...");   
    //Get grouped data
        //For now assuming data can be filtered independently
    cv.doFilter(limiter);
    cv.doGrouping();
    rc.doFilter(limiter);
    rc.doGrouping();
    System.out.println("Done with grouping!!");
    
    //Do join
    joinRes = cv.getGroupedCvPRDD().join(rc.getGroupedRCPRDD());
    JavaPairRDD.toRDD(joinRes).toJavaRDD().map(Utils.joinMapper);
    spsn.stop();
}

}
