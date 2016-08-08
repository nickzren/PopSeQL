/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.utils;

import com.atav.genotypes.conf.Configuration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
    public static Set<String> sampleSet;
    
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
        sampleSet=new HashSet<>(SampleManager.sampleIds);
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }
    
    
    
    public List<String> fetchSampleIds(){
        
        return spsn
                .read()
                .format("jdbc")
                .options(options)
                .load()
                .toJavaRDD()
                .map((Row t) -> Integer.toString(t.getInt(0)))
                .collect();
                //.toArray(new String[0]);
                
    }
    
}
