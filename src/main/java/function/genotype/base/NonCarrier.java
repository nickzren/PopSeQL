package function.genotype.base;

import global.Data;
import global.Index;

/**
 *
 * @author nick
 */
public class NonCarrier {

    public int sampleId;
    public byte genotype;
    public int coverage;
    public byte samplePheno;

    public static short getCovValue(char letter) {
        switch (letter) {
            case 'a':
            default:
                return Data.SHORT_NA;
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

    public NonCarrier(int sample_id, short cov, byte pheno) {
        sampleId = sample_id;
        coverage = cov;
        samplePheno = pheno;
        if (coverage == Data.SHORT_NA) {
            genotype = Data.BYTE_NA;
        } else {
            genotype = Index.REF;
        }
    }

    public byte getSamplePheno() {
        return samplePheno;
    }

    public int getSampleId() {
        return sampleId;
    }

    public byte getGenotype() {
        return genotype;
    }

    public int getCoverage() {
        return coverage;
    }
}
