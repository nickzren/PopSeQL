/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.beans;

import java.io.Serializable;

/**
 *
 * @author kaustubh
 */
public class Carrier implements Serializable {
    
    //just a few fields for prototype
    private String variantId;
    private String sampleId;
    private String blockId;
    private String chr;
    private String ref;
    private String pos;
    private String alt;
    
    private static final long serialVersionUID = 42L;        
    private String haploScore;
    private String genotype;
    private String qcFailCase;
    private String qcFailCtrl;

    public String getHaploScore() {
        return haploScore;
    }

    public void setHaploScore(String haploScore) {
        this.haploScore = haploScore;
    }

    public String getGenotype() {
        return genotype;
    }

    public void setGenotype(String genotype) {
        this.genotype = genotype;
    }

    public String getQcFailCase() {
        return qcFailCase;
    }

    public void setQcFailCase(String qcFailCase) {
        this.qcFailCase = qcFailCase;
    }

    public String getQcFailCtrl() {
        return qcFailCtrl;
    }

    public void setQcFailCtrl(String qcFailCtrl) {
        this.qcFailCtrl = qcFailCtrl;
    }

    public String getVariantId() {
        return variantId;
    }

    public void setVariantId(String variantId) {
        this.variantId = variantId;
    }

    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public String getAlt() {
        return alt;
    }

    public void setAlt(String alt) {
        this.alt = alt;
    }
    
    
            
}
