package function.genotype.base;

import global.Data;
import global.Index;

/**
 *
 * @author nick
 */
public class NonCarrier {

    public int sampleId;
    public byte GT;
    public short DPbin;
    public byte samplePheno;
    
    public static boolean isValidDpBin(char letter){
        switch (letter) {
            case 'a':
            case 'b':
            case 'c':
            case 'd':
            case 'e':
            case 'f':
            case 'g':
                return true;
            default:
                return false;
        }
    }

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
                return 30;
            case 'f':
                return 50;
            case 'g':
                return 200;
        }
    }

    public NonCarrier() {
    }

    public NonCarrier(int sample_id, short cov, byte pheno) {
        sampleId = sample_id;
        DPbin = cov;
        samplePheno = pheno;
        if (DPbin == Data.SHORT_NA) {
            GT = Data.BYTE_NA;
        } else {
            GT = Index.REF;
        }
    }

    public byte getSamplePheno() {
        return samplePheno;
    }

    public int getSampleId() {
        return sampleId;
    }

    public byte getGenotype() {
        return GT;
    }

    public short getDPBin() {
        return DPbin;
    }
}
