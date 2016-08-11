package function.genotype.base;

import global.Data;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 * @author felipe
 */
public class NonCarrier {
    
    public int sampleId;
    public int genotype;
    public int coverage;
    public int samplePheno;
    
    public NonCarrier() {   
    }
    
    public NonCarrier(Row r) {
        sampleId = r.getInt(r.fieldIndex("sample_id"));
        coverage = r.getShort(r.fieldIndex("coverage"));
        samplePheno = r.getShort(r.fieldIndex("pheno"));
        if (coverage == Data.NA) {
            genotype = Data.NA;
        } else {
            genotype = 0;
        }
    }
    
    public NonCarrier(int sample_id, short cov, short pheno) {
        sampleId = sample_id;
        coverage = cov;
        samplePheno = pheno;
        if (coverage == Data.NA) {
            genotype = Data.NA;
        } else {
            genotype = 0;
        }
    }
    
    public String simpleString(int variantId) {
        return Integer.toString(variantId)+","+
                Integer.toString(sampleId)+","+
                Integer.toString(genotype)+","+
                Integer.toString(coverage);
    }
    
    public int getSamplePheno() {
        return samplePheno;
    }
    
    /* Copied from ATAV code */
    public int getSampleId() {
        return sampleId;
    }

    public int getGenotype() {
        return genotype;
    }

    public int getCoverage() {
        return coverage;
    }
    /* ---------- */
    
}
