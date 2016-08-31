/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.beans;

import java.io.Serializable;
import java.math.BigDecimal;
import org.apache.spark.sql.Row;

/**
 *
 * @author kaustubh
 */
public class Carrier implements Serializable {
    
    //just a few fields for prototype
    private String variantId;
    private String sampleId;
//    private String blockId;
//    private String chr;
//    private String ref;
//    private int pos;
//    private String alt;
    
    private static final long serialVersionUID = 42L;        
//    private String haploScore;
      private int genotype;
//    private String qcFailCase;
//    private String qcFailCtrl;
    private int samToolsCov;
    private int gatkFiltCov;
    private int readsRef;
    private int readsAlt;
    private float vqslod;
    private double genoQualGQ;
    private double strBiasFS;
    private double haploScore;
    private double rmsMapQualMq;
    private double qualByDepthQD;
    private BigDecimal qual;
    private double rdPosRnkSum;
    private double mapQualRnkSum;
    private String passFailStatus;
    private String pheno;
    
    public Carrier(Row r) {
        this.variantId = Integer.toString(r.getInt(2));
//        this.sampleId = Integer.toString(r.getInt(1));
//        this.blockId = r.getString(0);
//        this.chr = r.getString(3);
//        this.pos = r.getInt(4);
//        this.ref = r.getString(5);
        this.genotype= r.getInt(7);
                
        this.genotype=r.getInt(7);
        this.samToolsCov=r.getInt(8);
        this.gatkFiltCov=r.getInt(9);
        this.readsAlt=r.getInt(11);
        this.readsRef=r.getInt(10);
        this.vqslod=(!r.isNullAt(12))? r.getFloat(12):0f;
        this.genoQualGQ=r.getDouble(13);
        this.strBiasFS=r.getDouble(14);
        this.haploScore=r.getDouble(15);
        this.rmsMapQualMq=r.getDouble(16);
        this.qualByDepthQD=r.getDouble(17);
        this.qual=r.getDecimal(18);
        this.rdPosRnkSum=(!r.isNullAt(19))?r.getDouble(19):0d;
        this.mapQualRnkSum=(!r.isNullAt(20))?r.getDouble(20):0d;
        this.passFailStatus=r.getString(22);
    }

    
//    
//    public String getHaploScore() {
//        return haploScore;
//    }
//
//    public void setHaploScore(String haploScore) {
//        this.haploScore = haploScore;
//    }

    public int getGenotype() {
        return genotype;
    }



    public void setGenotype(Short genotype) {
        this.genotype = genotype;
    }


    

//    public String getQcFailCase() {
//        return qcFailCase;
//    }
//
//    public void setQcFailCase(String qcFailCase) {
//        this.qcFailCase = qcFailCase;
//    }
//
//    public String getQcFailCtrl() {
//        return qcFailCtrl;
//    }
//
//    public void setQcFailCtrl(String qcFailCtrl) {
//        this.qcFailCtrl = qcFailCtrl;
//    }

    public String getVariantId() {
        return variantId;
    }

    public void setVariantId(String variantId) {
        this.variantId = variantId;
    }
//
//    public String getSampleId() {
//        return sampleId;
//    }
//
//    public void setSampleId(String sampleId) {
//        this.sampleId = sampleId;
//    }
//
//    public String getBlockId() {
//        return blockId;
//    }
//
//    public void setBlockId(String blockId) {
//        this.blockId = blockId;
//    }
//
//    public String getChr() {
//        return chr;
//    }
//
//    public void setChr(String chr) {
//        this.chr = chr;
//    }
//
//    public String getRef() {
//        return ref;
//    }
//
//    public void setRef(String ref) {
//        this.ref = ref;
//    }
//
//    public int getPos() {
//        return pos;
//    }
//
//    public void setPos(int pos) {
//        this.pos = pos;
//    }

//    public String getAlt() {
//        return alt;
//    }
//
//    public void setAlt(String alt) {
//        this.alt = alt;
//    }

    public String getSampleId() {
        return sampleId;
    }

    public int getSamToolsCov() {
        return samToolsCov;
    }

    public int getGatkFiltCov() {
        return gatkFiltCov;
    }

    public int getReadsRef() {
        return readsRef;
    }

    public int getReadsAlt() {
        return readsAlt;
    }

    public float getVqslod() {
        return vqslod;
    }

    public double getGenoQualGQ() {
        return genoQualGQ;
    }

    public double getStrBiasFS() {
        return strBiasFS;
    }

    public double getHaploScore() {
        return haploScore;
    }

    public double getRmsMapQualMq() {
        return rmsMapQualMq;
    }

    public double getQualByDepthQD() {
        return qualByDepthQD;
    }

    public BigDecimal getQual() {
        return qual;
    }

    public double getRdPosRnkSum() {
        return rdPosRnkSum;
    }

    public double getMapQualRnkSum() {
        return mapQualRnkSum;
    }

    public String getPassFailStatus() {
        return passFailStatus;
    }

    public String getPheno() {
        return pheno;
    }
    
    
    
            
}
