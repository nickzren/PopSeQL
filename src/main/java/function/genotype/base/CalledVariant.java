package function.genotype.base;

import function.variant.base.Output;
import function.variant.base.Region;
import global.Data;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collection;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 * @author felipe
 */
public class CalledVariant extends Region {
    
    private HashMap<Integer, Carrier> carrierMap = new HashMap<>();
    private HashMap<Integer, NonCarrier> noncarrierMap = new HashMap<>();
//    private int[] genotype = new int[SampleManager.getListSize()];
//    private int[] coverage = new int[SampleManager.getListSize()];
    private int[] qcFailSample = {0,0};
    
    
    public int variantId;
    public String variantIdStr;
    public String allele;
    public String refAllele;
    //public String rsNumber;
    //public float cscorePhred;
    //Indel attributes
//    public String indelType;
    private boolean isIndel;
    
    public short blockOffset;
    
    public CalledVariant() {
        
    };
    
    public CalledVariant(int vid, Iterator<Row> carrierDataIt,Iterator<Row> nonCarrierDataIt) {
        variantId = vid;
        Row r = null;
        while(carrierDataIt.hasNext()) {
            r = carrierDataIt.next();
            Carrier c = new Carrier(r);
            if(c.getCoverage() == Data.NA)
                qcFailSample[c.getSamplePheno()]++;
            else
                carrierMap.put(r.getInt(r.fieldIndex("sample_id")), c);
        }
        initVariantData(r);
        
        while(nonCarrierDataIt.hasNext()) {
            r = nonCarrierDataIt.next();
            NonCarrier nc = new NonCarrier(r);
            noncarrierMap.put(r.getInt(r.fieldIndex("sample_id")), nc);
        }
        
    }
    
    public void addCarrier(Row r, int sampleId, short pheno) {
        // TODO: add filter
        //if(r.getInt(r.fieldIndex("samtools_raw_coverage")) )
        carrierMap.put(sampleId, new Carrier(r,pheno));
    }
    
    public void addNonCarrier(int sampleId, short coverage, short pheno) {
        noncarrierMap.put(sampleId, new NonCarrier(sampleId, coverage, pheno));
    }
    
    public void addSampleDataToOutput(Output output) {
        for (Carrier c : carrierMap.values()) {
//            if(c.getCoverage() != Data.NA)
            output.addSampleGeno(c.getGenotype(), c.getSamplePheno());
        }
        for(NonCarrier nc : noncarrierMap.values()) {
            output.addSampleGeno(nc.getGenotype(), nc.getSamplePheno());
        }
    }
    
    public void initVariantData(Row r) {
        chrStr = r.getString(r.fieldIndex("chr"));
        chrNum = intChr();
        
        allele = r.getString(r.fieldIndex("alt"));
        refAllele = r.getString(r.fieldIndex("ref"));
        
        int position = r.getInt(r.fieldIndex("pos"));
        
        variantIdStr =
                chrStr+"-"+Integer.toString(position)+"-"+
                refAllele+"-"+allele;
        
        isIndel = allele.length() != refAllele.length();
        
        // Magic trick to get block offset
        blockOffset = (short) ((position - 1) & 0x3FF);
        
        initRegion(chrStr,position, position);
        
    }
    
    public Collection<Carrier> getCarriers() {
        return carrierMap.values();
    }
            
//    public Iterator<String> getStringRowIterator() {
//        ArrayList<String> l = new ArrayList<>(carrierMap.size()+noncarrierMap.size());
//        for(Carrier c : carrierMap.values())
//            l.add(c.simpleString(variantId));
//        for(NonCarrier nc : noncarrierMap.values())
//            l.add(nc.simpleString(variantId));
//        return l.iterator();
//    }
    
    private int intChr() {
        if (chrStr.equals("X")
                || chrStr.equals("XY")) {
            return 23;
        } else if (chrStr.equals("Y")) {
            return 24;
        } else if (chrStr.equals("MT")) {
            return 26;
        } else {
            try{
                return Integer.parseInt(chrStr);
            } catch (NumberFormatException e) {
                return Data.NA;
            }
        }
    }
    
    /* Code from ATAV's Variant.java */
    public int getVariantIdNegative4Indel() {
        return variantId;
    }

    public String getVariantIdStr() {
        return variantIdStr;
    }

    public String getType() {
        if (isIndel) {
            return "indel";
        } else {
            return "snv";
        }
    }

    public String getAllele() {
        return allele;
    }

    public String getRefAllele() {
        return refAllele;
    }
    
    public boolean isSnv() {
        return !isIndel;
    }

    public boolean isIndel() {
        return isIndel;
    }

    public boolean isDel() {
        return refAllele.length() > allele.length();
    }
    /* --------- */
    
    
    /* Code from ATAV's CalledVariant.java */
    public int getQcFailSample(int pheno) {
        return qcFailSample[pheno];
    }
    
    /* ---------- */
    
}
