package function.variant.base;

import function.genotype.base.CalledVariant;
import function.genotype.base.GenotypeLevelFilterCommand;
import global.Data;
import global.Index;
import function.genotype.statistics.HWEExact;
import utils.MathManager;

/**
 *
 * @author nick
 */
public class Output /* implements Cloneable */ {

    protected CalledVariant calledVar;

    protected boolean isMinorRef = false; // reference allele is minor or major

    protected int[][] genoCount = new int[3][2];
    protected int[] minorHomCount = new int[2];
    protected int[] majorHomCount = new int[2];
    protected float[] hetFreq = new float[2];
    protected float[] minorAlleleFreq = new float[2];
    protected float[] minorHomFreq = new float[2];
    protected double[] hweP = new double[2];

    public Output(CalledVariant c) {
        calledVar = c;
    }

    public Output() {

    }

    public CalledVariant getCalledVar() {
        return calledVar;
    }

    public boolean isIsMinorRef() {
        return isMinorRef;
    }

    public int[][] getGenoCount() {
        return genoCount;
    }

    public int[] getMinorHomCount() {
        return minorHomCount;
    }

    public int[] getMajorHomCount() {
        return majorHomCount;
    }

    public float[] getHetFreq() {
        return hetFreq;
    }

    public float[] getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public float[] getMinorHomFreq() {
        return minorHomFreq;
    }

    public double[] getHweP() {
        return hweP;
    }

    public void addSampleGeno(byte geno, int pheno) {
        if (geno != Data.BYTE_NA) {
            genoCount[geno][pheno]++;
        }
    }
    
    public void calculate() {
        calculateAlleleFreq();

        calculateGenotypeFreq();

        calculateHweP();

        countMajorMinorHomHet();
    }

    private void calculateAlleleFreq() {
        int caseAC = 2 * genoCount[Index.HOM][Index.CASE]
                + genoCount[Index.HET][Index.CASE];
        int caseTotalAC = caseAC + genoCount[Index.HET][Index.CASE]
                + 2 * genoCount[Index.REF][Index.CASE];

        float caseAF = MathManager.devide(caseAC, caseTotalAC); // (2*hom + het) / (2*hom + 2*het + 2*ref)

        minorAlleleFreq[Index.CASE] = caseAF;
        if (caseAF > 0.5) {
            minorAlleleFreq[Index.CASE] = 1.0f - caseAF;
        }

        int ctrlAC = 2 * genoCount[Index.HOM][Index.CTRL]
                + genoCount[Index.HET][Index.CTRL];
        int ctrlTotalAC = ctrlAC + genoCount[Index.HET][Index.CTRL]
                + 2 * genoCount[Index.REF][Index.CTRL];

        float ctrlAF = MathManager.devide(ctrlAC, ctrlTotalAC);

        minorAlleleFreq[Index.CTRL] = ctrlAF;
        if (ctrlAF > 0.5) {
            isMinorRef = true;
            minorAlleleFreq[Index.CTRL] = 1.0f - ctrlAF;
        } else {
            isMinorRef = false;
        }
    }

    private void calculateGenotypeFreq() {
        int totalCaseGenotypeCount
                = genoCount[Index.HOM][Index.CASE]
                + genoCount[Index.HET][Index.CASE]
                + genoCount[Index.REF][Index.CASE];

        int totalCtrlGenotypeCount
                = genoCount[Index.HOM][Index.CTRL]
                + genoCount[Index.HET][Index.CTRL]
                + genoCount[Index.REF][Index.CTRL];

        // hom / (hom + het + ref)
        if (isMinorRef) {
            minorHomFreq[Index.CASE] = MathManager.devide(genoCount[Index.REF][Index.CASE], totalCaseGenotypeCount);

            minorHomFreq[Index.CTRL] = MathManager.devide(genoCount[Index.REF][Index.CTRL], totalCtrlGenotypeCount);
        } else {
            minorHomFreq[Index.CASE] = MathManager.devide(genoCount[Index.HOM][Index.CASE], totalCaseGenotypeCount);

            minorHomFreq[Index.CTRL] = MathManager.devide(genoCount[Index.HOM][Index.CTRL], totalCtrlGenotypeCount);
        }

        hetFreq[Index.CASE] = MathManager.devide(genoCount[Index.HET][Index.CASE], totalCaseGenotypeCount);
        hetFreq[Index.CTRL] = MathManager.devide(genoCount[Index.HET][Index.CTRL], totalCtrlGenotypeCount);
    }

    public void calculateHweP() {
        hweP[Index.CASE] = HWEExact.getP(genoCount[Index.HOM][Index.CASE],
                genoCount[Index.HET][Index.CASE],
                genoCount[Index.REF][Index.CASE]);

        hweP[Index.CTRL] = HWEExact.getP(genoCount[Index.HOM][Index.CTRL],
                genoCount[Index.HET][Index.CTRL],
                genoCount[Index.REF][Index.CTRL]);
    }

    public void countMajorMinorHomHet() {
        if (isMinorRef) {
            minorHomCount[Index.CASE] = genoCount[Index.REF][Index.CASE];
            minorHomCount[Index.CTRL] = genoCount[Index.REF][Index.CTRL];
            majorHomCount[Index.CASE] = genoCount[Index.HOM][Index.CASE];
            majorHomCount[Index.CTRL] = genoCount[Index.HOM][Index.CTRL];
        } else {
            minorHomCount[Index.CASE] = genoCount[Index.HOM][Index.CASE];
            minorHomCount[Index.CTRL] = genoCount[Index.HOM][Index.CTRL];
            majorHomCount[Index.CASE] = genoCount[Index.REF][Index.CASE];
            majorHomCount[Index.CTRL] = genoCount[Index.REF][Index.CTRL];
        }
    }

    public String getGenoStr(byte geno) {
        switch (geno) {
            case Index.HOM:
                return "hom";
            case Index.HET:
                return "het";
            case Index.REF:
                return "hom ref";
            case Data.BYTE_NA:
                return Data.STRING_NA;
        }

        return "";
    }

    protected int getVarPresent() {
        if (GenotypeLevelFilterCommand.isAllNonRef && isMinorRef) {
            return majorHomCount[Index.CASE]
                    + genoCount[Index.HET][Index.CASE]
                    + majorHomCount[Index.CTRL]
                    + genoCount[Index.HET][Index.CTRL];
        }

        return minorHomCount[Index.CASE]
                + genoCount[Index.HET][Index.CASE]
                + minorHomCount[Index.CTRL]
                + genoCount[Index.HET][Index.CTRL];
    }

    protected int getCaseCarrier() {
        if (GenotypeLevelFilterCommand.isAllNonRef && isMinorRef) {
            return majorHomCount[Index.CASE]
                    + genoCount[Index.HET][Index.CASE];
        }

        return minorHomCount[Index.CASE]
                + genoCount[Index.HET][Index.CASE];
    }

    /*
     * if ref is minor then only het & ref are qualified samples. If ref is
     * major then only hom & het are qualified samples.
     */
    public boolean isQualifiedGeno(byte geno) {
        if (GenotypeLevelFilterCommand.isAllGeno) {
            return true;
        }

        if (GenotypeLevelFilterCommand.isAllNonRef) {
            if (geno == Index.HOM || geno == Index.HET) {
                return true;
            }
        }

        if (isMinorRef) {
            if (geno == Index.REF || geno == Index.HET) {
                return true;
            }
        } else if (geno == Index.HOM || geno == Index.HET) {
            return true;
        }

        return false;
    }

    public boolean isMinorRef() {
        return isMinorRef;
    }
}
