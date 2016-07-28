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
    private String readCoverage;
    private static final long serialVersionUID = 41L;
    
    public String getReadCoverage() {
        return readCoverage;
    }

    public void setReadCoverage(String readCoverage) {
        this.readCoverage = readCoverage;
    }
    
}
