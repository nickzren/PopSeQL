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
public class NonCarrier implements Serializable{
    
    private String sample_id;
    private short coverageVal;
    private short genotype=0;
    
    private static final long serialVersionUID = 41L;

    public NonCarrier(String samp, short covVal) {
        this.sample_id = samp;
        this.coverageVal=covVal;
    }
    
    public String getSample_id() {
        return sample_id;
    }

    public short getGenotype() {
        return genotype;
    }

    public void setGenotype(short genotype) {
        this.genotype = genotype;
    }

    
    
    public void setSample_id(String sample_id) {
        this.sample_id = sample_id;
    }

    public short getCoverageVal() {
        return coverageVal;
    }

    public void setCoverageVal(short coverageVal) {
        this.coverageVal = coverageVal;
    }
    
    
}
