/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.beans;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 *
 * @author kaustubh
 */
public class Variant implements Serializable {
    private String variantID;
    private String sampleID;
    private Map<String, Carrier> carrierMap;
    private Map<String, NonCarrier> nonCarrierMap;
    private static final long serialVersionUID = 40L;
    
    public Variant(Row r) {
        this.variantID = Integer.toString(r.getInt(2));
        this.sampleID = Integer.toString(r.getInt(1));
        if (null==this.carrierMap)this.carrierMap = new HashMap<>();
        this.carrierMap.put(this.sampleID, new Carrier(r));
        if (null== this.nonCarrierMap)this.nonCarrierMap = new HashMap<>();
    }

    public String getSampleID() {
        return sampleID;
    }

    public void setSampleID(String sampleID) {
        this.sampleID = sampleID;
    }
    
    

    public String getVariantID() {
        return variantID;
    }

    public void setVariantID(String variantID) {
        this.variantID = variantID;
    }

    public Map<String, Carrier> getCarrierMap() {
        return carrierMap;
    }

    public void setCarrierMap(Map<String, Carrier> carrierMap) {
        this.carrierMap = carrierMap;
    }

    public Map<String, NonCarrier> getNonCarrierMap() {
        return nonCarrierMap;
    }

    public void setNonCarrierMap(Map<String, NonCarrier> nonCarrierMap) {
        this.nonCarrierMap = nonCarrierMap;
    }
    
    
}
