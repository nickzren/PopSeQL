package function.genotype.base;

import global.Data;
import global.Index;
import utils.MathManager;

/**
 *
 * @author nick
 */
public class Output {

    protected CalledVariant calledVar;

    protected int[][] genoCount = new int[3][2];
    protected float[] hetFreq = new float[2];
    protected float[] alleleFreq = new float[2];
    protected float[] homFreq = new float[2];

    public Output(CalledVariant c) {
        calledVar = c;
    }

    public Output() {

    }

    public CalledVariant getCalledVar() {
        return calledVar;
    }

    public int[][] getGenoCount() {
        return genoCount;
    }

    public float[] getHetFreq() {
        return hetFreq;
    }

    public float[] getMinorAlleleFreq() {
        return alleleFreq;
    }

    public float[] getMinorHomFreq() {
        return homFreq;
    }

    public void addSampleGeno(byte geno, int pheno) {
        if (geno != Data.BYTE_NA) {
            genoCount[geno][pheno]++;
        }
    }

    public void calculate() {
        calculateAlleleFreq();

        calculateGenotypeFreq();
    }

    private void calculateAlleleFreq() {
        int caseAC = 2 * genoCount[Index.HOM][Index.CASE]
                + genoCount[Index.HET][Index.CASE];
        int caseTotalAC = caseAC + genoCount[Index.HET][Index.CASE]
                + 2 * genoCount[Index.REF][Index.CASE];

        alleleFreq[Index.CASE] = MathManager.devide(caseAC, caseTotalAC); // (2*hom + het) / (2*hom + 2*het + 2*ref)

        int ctrlAC = 2 * genoCount[Index.HOM][Index.CTRL]
                + genoCount[Index.HET][Index.CTRL];
        int ctrlTotalAC = ctrlAC + genoCount[Index.HET][Index.CTRL]
                + 2 * genoCount[Index.REF][Index.CTRL];

        alleleFreq[Index.CTRL] = MathManager.devide(ctrlAC, ctrlTotalAC);
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
        homFreq[Index.CASE] = MathManager.devide(genoCount[Index.HOM][Index.CASE], totalCaseGenotypeCount);
        homFreq[Index.CTRL] = MathManager.devide(genoCount[Index.HOM][Index.CTRL], totalCtrlGenotypeCount);

        hetFreq[Index.CASE] = MathManager.devide(genoCount[Index.HET][Index.CASE], totalCaseGenotypeCount);
        hetFreq[Index.CTRL] = MathManager.devide(genoCount[Index.HET][Index.CTRL], totalCtrlGenotypeCount);
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

    public boolean isQualifiedGeno(byte geno) {
        return geno == Index.HOM || geno == Index.HET;
    }

    private int getVarPresent() {
        return genoCount[Index.HOM][Index.CASE]
                + genoCount[Index.HET][Index.CASE]
                + genoCount[Index.HOM][Index.CTRL]
                + genoCount[Index.HET][Index.CTRL];
    }

    public boolean isValid() {
        return GenotypeLevelFilterCommand.isMinVarPresentValid(getVarPresent())
                && GenotypeLevelFilterCommand.isMaxCtrlMafValid(alleleFreq[Index.CTRL]);
    }
}
