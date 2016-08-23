/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

import com.atav.genotypes.beans.Variant;
import function.variant.base.Output;
import global.Index;
import java.io.Serializable;
import java.util.StringJoiner;
import utils.FormatManager;

/**
 *
 * @author kaustubh
 */
public class VarGenoOutput implements Serializable {
   
    /**
     *
     * @param v
     */
    private static final long serialVersionUID = 51L;
    private final Variant v;
    private final Output o ;
    //private final SampleManager sm;
    private int[] qcFailSample={0,0}; //This is not being updated for now
//    public static void testPheno(SampleManager sam){
//        System.out.println("Fetching Phenotypes .................................");
//        System.out.println(sam.broadCastPheno.value().toString());
//    }
    
    public VarGenoOutput(Variant v, Output o){
        //super(v);    
        this.v=v;
        this.o=o;
    }

    public int[] getQcFailSample() {
        return qcFailSample;
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
       sj.add(o.isMinorRef() ? "1" : "0");// Is Minor Ref
       sj.add(o.getGenoStr(v.getGenotype()));
       sj.add(v.getSampleID());
       sj.add(v.getPheno()); //Using Random Phenotypes for now
       sj.add(Integer.toString(o.getMajorHomCount()[Index.CASE]));
       sj.add(Integer.toString((o.getGenoCount()[Index.HET][Index.CASE])));
       sj.add(Integer.toString(o.getMinorHomCount()[Index.CASE]));
       sj.add(String.format("%.0f", o.getMinorHomFreq()[Index.CASE]));
       sj.add(String.format("%.0f", o.getHetFreq()[Index.CASE]));
       sj.add(Integer.toString(o.getMajorHomCount()[Index.CTRL]));
       sj.add(Integer.toString(o.getGenoCount()[Index.HET][Index.CTRL]));
       sj.add(Integer.toString(o.getMinorHomCount()[Index.CTRL]));
       sj.add(String.format("%.0f", o.getMinorHomFreq()[Index.CTRL]));
       sj.add(String.format("%.0f", o.getHetFreq()[Index.CTRL]));
       sj.add(Integer.toString(o.getGenoCount()[Index.MISSING][Index.CASE]));
       sj.add(Integer.toString(getQcFailSample()[Index.CASE]));
       sj.add(Integer.toString(o.getGenoCount()[Index.MISSING][Index.CTRL]));
       sj.add(Integer.toString(getQcFailSample()[Index.CTRL]));
       sj.add(Integer.toString(getQcFailSample()[Index.CTRL]));
       sj.add(String.format("%.0f", o.getMinorAlleleFreq()[Index.CASE]));
       sj.add(String.format("%.0f", o.getMinorAlleleFreq()[Index.CTRL]));
       sj.add(String.format("%.0f", o.getHweP()[Index.CASE]));
       sj.add(String.format("%.0f", o.getHweP()[Index.CTRL]));
       sj.add(Integer.toString(v.getSamToolsCov()));
       sj.add(Integer.toString(v.getGatkFiltCov()));
       sj.add(Integer.toString(v.getReadsAlt()));
       sj.add(Integer.toString(v.getReadsRef()));
       sj.add(FormatManager.getPercAltRead(v.getReadsAlt(),v.getGatkFiltCov()));
       sj.add(Float.toString(v.getVqslod()));
       sj.add(v.getPassFailStatus());
       sj.add(Double.toString(v.getGenoQualGQ()));
       sj.add(Double.toString(v.getStrBiasFS()));
       sj.add(Double.toString(v.getHaploScore()));
       sj.add(Double.toString(v.getRmsMapQualMq()));
       sj.add(Double.toString(v.getQualByDepthQD()));
       sj.add(v.getQual().toPlainString());
       sj.add(Double.toString(v.getRdPosRnkSum()));
       sj.add(Double.toString(v.getMapQualRnkSum()));
       
       return sj.toString();
      }
}
