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
    CalledVariant cv = new CalledVariant(spsn);
    String limiter="where block_id in (\"X-125694\",\"X-120643\",\"X-120619\",\"X-120080\", \"X-107153\")";
    cv.doFilter(limiter);
    cv.doGrouping();
    System.out.println(cv.getGroupedCvPRDD().toString());
}

}
