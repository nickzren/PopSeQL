package function.genotype.base;

import global.Data;

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
    
    public static short getCovValue(char letter) {
        switch (letter) {
            case 'a':
            default:
                return -1;
            case 'b':
                return 3;
            case 'c':
                return 10;
            case 'd':
                return 20;
            case 'e':
                return 201;
        }
    }
    
    public NonCarrier() {   
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
    
    public int getSamplePheno() {
        return samplePheno;
    }
    
    public int getSampleId() {
        return sampleId;
    }

    public int getGenotype() {
        return genotype;
    }

    public int getCoverage() {
        return coverage;
    }
}
