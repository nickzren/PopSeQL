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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;


public class Generate {
    //Init session
            
public static SparkSession spsn = SparkSession
  .builder()
  //.config("spark.some.config.option", "some-value")
  .appName("Genotype generator")
  .master(Configuration.master)
  .getOrCreate();


public static void main(String args[]){
    //Called Vars
    CalledVariant cv = new CalledVariant(spsn);
    //Read Cov
    ReadCoverage rc = new ReadCoverage(spsn);
    
    //Or use args with a utility to generate filter predicate 
    String limiter=" where block_id IN (\"X-125694\",\"X-120643\",\"X-120619\",\"X-120080\", \"X-107153\") ";

    System.out.println("Started grouping...");
    
    //Get grouped data
    cv.doFilter(limiter);
    cv.doGrouping();
    rc.doFilter(limiter);
    rc.doGrouping();
    
    //Do join
    JavaPairRDD p =cv.getGroupedCvPRDD().join(rc.getGroupedRCPRDD());
    
    System.out.println("Done with grouping!!");
    System.out.println(p.collect());
}

}
