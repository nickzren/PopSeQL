/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.utils;

import com.atav.genotypes.conf.Configuration;
import global.PopSpark;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author kaustubh
 */
public class SampleManager {
    Map<String, String> options;
    private String cvQuery = "Select distinct sample_id from " 
            + Configuration.schema + ".read_coverage";
    SparkSession spsn;
    public static List<String> sampleIds;
    public Broadcast<Set<String>> broadCastSamples;
    public Broadcast<Map<String,Integer>> broadCastPheno;

    public Broadcast<Map<String, Integer>> getBroadCastPheno() {
        return broadCastPheno;
    }

    public Broadcast<Set<String>> getBroadCastSamples() {
        return broadCastSamples;
    }
    
    
    public SampleManager(SparkSession sesh){
        spsn=sesh;
        options = new HashMap<>();
        options.put("url", Configuration.url);
        options.put("dbtable", "("+ cvQuery+") as t"); //default
        options.put("driver", Configuration.driver);
    }
    
    public void doFiltering(String args){
        cvQuery = cvQuery + args;
        cvQuery = "("+ cvQuery +") as t";                
        options.remove("dbtable");
        options.put("dbtable", cvQuery);
    }

    public void setSampleIds() {
        SampleManager.sampleIds = fetchSampleIds();
        //RandomPheno
        HashMap<String,Integer> phenoMap= new HashMap<>();
        for (String s: SampleManager.sampleIds){
            phenoMap.put(s, (new Random()).nextInt(2));
        }
        broadCastSamples= spsn.sparkContext().broadcast(new HashSet<>(SampleManager.sampleIds), scala.reflect.ClassTag$.MODULE$.apply(HashSet.class));
        broadCastPheno= spsn.sparkContext().broadcast(phenoMap, scala.reflect.ClassTag$.MODULE$.apply(HashMap.class));
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }
    
    
    public List<String> fetchSampleIds(){
        
//        return spsn
//                .read()
//                .format("jdbc")
//                .options(options)
//                .load()
//                .toJavaRDD()
//                .map((Row t) -> Integer.toString(t.getInt(0)))
//                .collect();
                //.toArray(new String[0]);
                 return spsn
                .read()
                .option("header", "false")
                .option("delimiter", "\t")
                .schema(
                        new StructType()
                        .add("id", DataTypes.IntegerType, false)
                        .add("pheno", DataTypes.ShortType, false)
                ).
                csv(Configuration.sample)
                .toJavaRDD()
                         .map((Row t) -> Integer.toString(t.getInt(0)))
                         .collect();
                         
    }
    
}
