/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes;

import com.atav.genotypes.beans.Carrier;
import com.atav.genotypes.beans.NonCarrier;
import com.atav.genotypes.beans.Variant;
import function.variant.base.Output;
import global.Index;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
    private static final String NA="NA";
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
        
    
    
      public List<String> getStringList(){
       ArrayList<String> strVals=new ArrayList<>(); 
       v.getCarrierMap().values().stream().forEach((c) -> {
           StringJoiner sj = new StringJoiner(",");
           StringJoiner sv = (new StringJoiner("-")).add(v.getChr()).add(Integer.toString(v.getPos())).add(v.getRef()).add(v.getAlt());
           sj.add(sv.toString());
           sj.add((v.getRef().trim().length()==1 && v.getAlt().trim().length()==1)?"SNV":"INDEL");
           sj.add(v.getRef());
           sj.add(v.getAlt());
           sj.add(o.isMinorRef() ? "1" : "0");// Is Minor Ref
           sj.add(o.getGenoStr(c.getGenotype()));
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
           sj.add(Integer.toString(c.getSamToolsCov()));
           sj.add(Integer.toString(c.getGatkFiltCov()));
           sj.add(Integer.toString(c.getReadsAlt()));
           sj.add(Integer.toString(c.getReadsRef()));
           sj.add(FormatManager.getPercAltRead(c.getReadsAlt(),c.getGatkFiltCov()));
           sj.add(Float.toString(c.getVqslod()));
           sj.add(c.getPassFailStatus());
           sj.add(Double.toString(c.getGenoQualGQ()));
           sj.add(Double.toString(c.getStrBiasFS()));
           sj.add(Double.toString(c.getHaploScore()));
           sj.add(Double.toString(c.getRmsMapQualMq()));
           sj.add(Double.toString(c.getQualByDepthQD()));
           sj.add(c.getQual().toPlainString());
           sj.add(Double.toString(c.getRdPosRnkSum()));
           sj.add(Double.toString(c.getMapQualRnkSum()));
           strVals.add(sj.toString());
        });
       
       for(NonCarrier nc : v.getNonCarrierMap().values()){
           StringJoiner sj = new StringJoiner(",");
           StringJoiner sv = (new StringJoiner("-")).add(v.getChr()).add(Integer.toString(v.getPos())).add(v.getRef()).add(v.getAlt());
           sj.add(sv.toString());
           sj.add((v.getRef().trim().length()==1 && v.getAlt().trim().length()==1)?"SNV":"INDEL");
           sj.add(v.getRef());
           sj.add(v.getAlt());
           sj.add(o.isMinorRef() ? "1" : "0");// Is Minor Ref
           sj.add(o.getGenoStr(nc.getGenotype()));
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
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           sj.add(NA);
           strVals.add(sj.toString());
       }
       
       return strVals;
      }
      
      
          public static StructType getSchema() {
        StructField[] fields = {
            DataTypes.createStructField("Variant ID", DataTypes.StringType, true),
            DataTypes.createStructField("Variant Type", DataTypes.StringType, true),
            DataTypes.createStructField("Ref Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Alt Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Is Minor Ref", DataTypes.IntegerType, true),
            DataTypes.createStructField("Genotype", DataTypes.StringType, true),
            DataTypes.createStructField("Sample Name", DataTypes.IntegerType, true),
            DataTypes.createStructField("Sample Type", DataTypes.StringType, true),
            DataTypes.createStructField("Major Hom Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Het Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Case Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Het Case Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Major Hom Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Het Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Ctrl Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Het Ctrl Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Missing Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("QC Fail Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Missing Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("QC Fail Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Case Maf", DataTypes.DoubleType, true),//d
            DataTypes.createStructField("Ctrl Maf", DataTypes.DoubleType, true),//d
            DataTypes.createStructField("Case HWE_P", DataTypes.StringType, true),//d
            DataTypes.createStructField("Ctrl HWE_P", DataTypes.StringType, true),//d
            DataTypes.createStructField("Samtools Raw Coverage", DataTypes.StringType, true),//s
            DataTypes.createStructField("Gatk Filtered Coverage", DataTypes.StringType, true),//s
            DataTypes.createStructField("Reads Alt", DataTypes.IntegerType, true),//s
            DataTypes.createStructField("Reads Ref", DataTypes.IntegerType, true),//s
            DataTypes.createStructField("Percent Alt Read", DataTypes.StringType, true),//d
            DataTypes.createStructField("Vqslod", DataTypes.StringType, true),//d
            DataTypes.createStructField("Pass Fail Status", DataTypes.StringType, true),
            DataTypes.createStructField("Genotype Qual GQ", DataTypes.StringType, true),//d
            DataTypes.createStructField("Strand Bias FS", DataTypes.StringType, true),//d
            DataTypes.createStructField("Haplotype Score", DataTypes.StringType, true),//d
            DataTypes.createStructField("Rms Map Qual MQ", DataTypes.StringType, true),//d
            DataTypes.createStructField("Qual By Depth QD", DataTypes.StringType, true),//d
            DataTypes.createStructField("Qual", DataTypes.StringType, true),//d
            DataTypes.createStructField("Read Pos Rank Sum", DataTypes.StringType, true),//d
            DataTypes.createStructField("Map Qual Rank Sum", DataTypes.StringType, true)
            
        };
        return new StructType(fields);
    }
}
