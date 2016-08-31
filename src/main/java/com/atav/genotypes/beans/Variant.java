/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.beans;

import java.io.Serializable;
import java.math.BigDecimal;
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
    private String chr;
    private String ref;
    private String alt;    
    private int pos;
    
//    private int samToolsCov;
//    private int gatkFiltCov;
//    private int readsRef;
//    private int readsAlt;
//    private float vqslod;
//    private double genoQualGQ;
//    private double strBiasFS;
//    private double haploScore;
//    private double rmsMapQualMq;
//    private double qualByDepthQD;
//    private BigDecimal qual;
//    private double rdPosRnkSum;
//    private double mapQualRnkSum;
//    private String passFailStatus;
    private String pheno;
    private Map<String, Carrier> carrierMap;
    private Map<String, NonCarrier> nonCarrierMap;
    private static final long serialVersionUID = 40L;
//    private int genotype;
    
    
    public Variant(Row r) {
        this.variantID = Integer.toString(r.getInt(2));
        this.sampleID = Integer.toString(r.getInt(1));
        if (null==this.carrierMap)this.carrierMap = new HashMap<>();
        this.carrierMap.put(this.sampleID, new Carrier(r));
        if (null== this.nonCarrierMap)this.nonCarrierMap = new HashMap<>();                
        this.pos = r.getInt(4);        
        this.ref=r.getString(5);
        this.chr=r.getString(3);
        this.alt=r.getString(6);
//        
//        this.genotype=r.getInt(7);
//        this.samToolsCov=r.getInt(8);
//        this.gatkFiltCov=r.getInt(9);
//        this.readsAlt=r.getInt(11);
//        this.readsRef=r.getInt(10);
//        this.vqslod=(!r.isNullAt(12))? r.getFloat(12):0f;
//        this.genoQualGQ=r.getDouble(13);
//        this.strBiasFS=r.getDouble(14);
//        this.haploScore=r.getDouble(15);
//        this.rmsMapQualMq=r.getDouble(16);
//        this.qualByDepthQD=r.getDouble(17);
//        this.qual=r.getDecimal(18);
//        this.rdPosRnkSum=(!r.isNullAt(19))?r.getDouble(19):0d;
//        this.mapQualRnkSum=(!r.isNullAt(20))?r.getDouble(20):0d;
//        this.passFailStatus=r.getString(22);
        
    }

    public String getPheno() {
        return pheno;
    }

    public void setPheno(String pheno) {
        this.pheno = pheno;
    }

    
//    public float getVqslod() {
//        return vqslod;
//    }
//
//    public double getGenoQualGQ() {
//        return genoQualGQ;
//    }
//
//    public double getStrBiasFS() {
//        return strBiasFS;
//    }
//
//    public double getHaploScore() {
//        return haploScore;
//    }
//
//    public double getRmsMapQualMq() {
//        return rmsMapQualMq;
//    }
//
//    public double getQualByDepthQD() {
//        return qualByDepthQD;
//    }
//
//    public BigDecimal getQual() {
//        return qual;
//    }
//
//    public double getRdPosRnkSum() {
//        return rdPosRnkSum;
//    }
//
//    public double getMapQualRnkSum() {
//        return mapQualRnkSum;
//    }
//
//    public String getPassFailStatus() {
//        return passFailStatus;
//    }
//    
//    
//
//    public int getReadsRef() {
//        return readsRef;
//    }
//
//    public int getReadsAlt() {
//        return readsAlt;
//    }
//
//    
//    public int getSamToolsCov() {
//        return samToolsCov;
//    }
//
//    public int getGatkFiltCov() {
//        return gatkFiltCov;
//    }
//
//    
//    
//    
//    public int getGenotype() {
//        return genotype;
//    }

    
    public String getAlt() {
        return alt;
    }

    
    
    public String getSampleID() {
        return sampleID;
    }

    public void setSampleID(String sampleID) {
        this.sampleID = sampleID;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public String getChr() {
        return chr;
    }

    public String getRef() {
        return ref;
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
