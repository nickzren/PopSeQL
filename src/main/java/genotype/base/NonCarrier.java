package genotype.base;

import global.Data;
import org.apache.spark.sql.Row;

/**
 *
 * @author ferocha
 */
public class NonCarrier {
    
    public int sampleId;
    public int genotype;
    public int coverage;
    
    public NonCarrier() {   
    }
    
    public NonCarrier(Row r) {
        sampleId = r.getInt(r.fieldIndex("sample_id"));
        coverage = r.getShort(r.fieldIndex("coverage"));
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
    
}
