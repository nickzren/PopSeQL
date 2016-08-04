/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

import com.atav.genotypes.conf.Configuration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
    
    private Dataset<Row> rcDF;
    private JavaRDD<Row> rcRDD;
    private JavaPairRDD<String, Row> rcPRDD;
    private JavaPairRDD<String, Map<String,TreeMap<Integer,String>>> transRCPRDD;
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

    public Dataset<Row> getRcDF() {
        return rcDF;
    }

    public void setRcDF() {
        rcDF = spsn
                .read()
                .format("jdbc")
                .options(options)
                .load();
    }
    
    public void setrcRDD(){
     if (rcDF==null){
         setRcDF();
     }
     rcRDD=rcDF.toJavaRDD();
    }
    
    public void setrcPRDD(){
        if(rcRDD==null){
            setrcRDD();
        }
        rcPRDD=rcRDD.mapToPair((Row r) -> {
            return new Tuple2<String, Row>(r.getString(0), r); // BlockID and Row
        } );
        
        transRCPRDD = rcPRDD.mapToPair((Tuple2<String, Row> t1) -> {
            Map<String, TreeMap<Integer, String>> m = new HashMap<>();
            boolean isNum = true;
            String covKey = "";
            TreeMap<Integer, String> res = new TreeMap<>();
            String[] rcVals = t1._2.getString(2)
                                    .split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)"); //Split Coverage string
            //Coverage range goes first, Val later
            for (String cov : rcVals) {
                if (isNum) {
                    covKey = cov;
                    isNum = false;
                } else {
                    res.put(new Integer(covKey), cov);
                    isNum = true;
                }
            }
            m.put(Integer.toString(t1._2.getInt(1)), //Sample ID
                    res); //Coverage TreeMap
            return new Tuple2<String, Map<String, TreeMap<Integer, String>>>(t1._1, m); //Block ID and Sample+Coverage Map
        });
    }
    
    public void doGrouping(){
        if(rcPRDD==null){
            setrcPRDD();
        }
        
        groupedRCPRDD=transRCPRDD.reduceByKey((Map<String, TreeMap<Integer, String>> t1, Map<String, TreeMap<Integer, String>> t2) -> {
            Map<String, TreeMap<Integer, String>> r= new HashMap<>();
            r.putAll(t1);
            r.putAll(t2);
            return r;
        });
    }

    public JavaPairRDD<String, Map<String, TreeMap<Integer, String>>> getGroupedRCPRDD() {
        return groupedRCPRDD;
    }
    
}
