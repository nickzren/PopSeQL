/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.beans;

import java.io.Serializable;
import java.util.TreeMap;
import org.apache.spark.sql.Row;

/**
 *
 * @author kaustubh
 */
public class NonCarrier implements Serializable{
    private String block_id;
    private String sample_id;
    private TreeMap<Integer,String> readCoverage;
    private static final long serialVersionUID = 41L;

    public NonCarrier(Row r) {
        this.block_id = r.getString(0);
        this.sample_id = Integer.toString(r.getInt(1));
        this.readCoverage = rcUncompress(r.getString(2));
    }
    
    public final TreeMap<Integer,String> rcUncompress (String s){
        boolean isNum=true;
        TreeMap<Integer,String> res = new TreeMap<>();
        String [] rcVals = s.split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
        //Coverage range goes first, Val later
        for (String cov : rcVals){
            String covKey="";
            if(isNum){
                covKey=cov;
                isNum=false;
            }else{
                res.put(new Integer(covKey), cov);
                isNum=true;
            }
        }
        return res;
    }

    public String getBlock_id() {
        return block_id;
    }

    public void setBlock_id(String block_id) {
        this.block_id = block_id;
    }

    public String getSample_id() {
        return sample_id;
    }

    public void setSample_id(String sample_id) {
        this.sample_id = sample_id;
    }
    
    
    
    
    public TreeMap getReadCoverage() {
        return readCoverage;
    }

    public void setReadCoverage(TreeMap readCoverage) {
        this.readCoverage = readCoverage;
    }
    
}
