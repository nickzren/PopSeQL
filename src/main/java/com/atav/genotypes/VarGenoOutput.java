/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

import com.atav.genotypes.beans.Variant;
import function.genotype.base.GenotypeLevelFilterCommand;
import com.atav.genotypes.utils.SampleManager;
import function.variant.base.Output;
import java.io.Serializable;
import java.util.HashMap;
import java.util.StringJoiner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author kaustubh
 */
public class VarGenoOutput extends Output implements Serializable {
   
    /**
     *
     * @param v
     */
    private static final long serialVersionUID = 51L;
    private final Variant v;
    
    public static void testPheno(){
        System.out.println("Fetching Phenotypes .................................");
        System.out.println(SampleManager.broadCastPheno.value().toString());
    }
    
    public VarGenoOutput(Variant v){
        super(v);    
        this.v=v;    
    }
    
    /**
     * 
     * @todo
     * toString()
     *  -> Get String Builder
     */
    
        public static String getTitle() {
        return "Variant ID,"
                + "Variant Type,"
                //+ "Rs Number,"
                + "Ref Allele,"
                + "Alt Allele,"
                //+ "CADD Score Phred,"
                //+ GerpManager.getTitle()
                //+ TrapManager.getTitle()
                + "Is Minor Ref,"
                + "Genotype,"
                + "Sample Name,"
                + "Sample Type,"
                + "Major Hom Case,"
                + "Het Case,"
                + "Minor Hom Case,"
                + "Minor Hom Case Freq,"
                + "Het Case Freq,"
                + "Major Hom Ctrl,"
                + "Het Ctrl,"
                + "Minor Hom Ctrl,"
                + "Minor Hom Ctrl Freq,"
                + "Het Ctrl Freq,"
                + "Missing Case,"
                + "QC Fail Case,"
                + "Missing Ctrl,"
                + "QC Fail Ctrl,"
                + "Case Maf,"
                + "Ctrl Maf,"
                + "Case HWE_P,"
                + "Ctrl HWE_P,"
                + "Samtools Raw Coverage,"
                + "Gatk Filtered Coverage,"
                + "Reads Alt,"
                + "Reads Ref,"
                + "Percent Alt Read,"
                //+ "Percent Alt Read Binomial P,"
                + "Vqslod,"
                + "Pass Fail Status,"
                + "Genotype Qual GQ,"
                + "Strand Bias FS,"
                + "Haplotype Score,"
                + "Rms Map Qual MQ,"
                + "Qual By Depth QD,"
                + "Qual,"
                + "Read Pos Rank Sum,"
                + "Map Qual Rank Sum,"
                //+ EvsManager.getTitle()
                //+ "Polyphen Humdiv Score,"
                //+ "Polyphen Humdiv Prediction,"
                //+ "Polyphen Humvar Score,"
                //+ "Polyphen Humvar Prediction,"
                //+ "Function,"
                //+ "Gene Name,"
                //+ "Artifacts in Gene,"
                //+ "Codon Change,"
                //+ "Gene Transcript (AA Change),"; 
                //+ ExacManager.getTitle()
                //+ KaviarManager.getTitle()
                //+ KnownVarManager.getTitle()
                //+ RvisManager.getTitle()
                //+ SubRvisManager.getTitle()
                //+ GenomesManager.getTitle()
                //+ MgiManager.getTitle()
                ;
    }
        
    
      public String toString(){
       StringJoiner sj = new StringJoiner(",");
       StringJoiner sv = (new StringJoiner("-")).add(v.getChr()).add(Integer.toString(v.getPos())).add(v.getRef()).add(v.getAlt());
       sj.add(sv.toString());
       sj.add((v.getRef().trim().length()==1 && v.getAlt().trim().length()==1)?"SNV":"INDEL");
       sj.add(v.getRef());
       sj.add(v.getAlt());
       // Is Minor Ref
       sj.add(super.getGenoStr(v.getGenotype()));
       sj.add(v.getSampleID());
       sj.add((SampleManager.broadCastPheno.value().get(v.getSampleID())==0)?"case":"ctrl"); //Using Random Phenotypes for now
       
       
       
       
       
       return sj.toString();
      }
}
