package function.genotype.collapsing;

import function.genotype.base.Sample;
import function.genotype.base.SampleManager;
import function.genotype.statistics.FisherExact;
import global.Data;
import utils.MathManager;

/**
 *
 * @author nick
 */
public class CollapsingSummary implements Comparable {

    String name; // gene name or region name

    int[] variantNumBySample = new int[SampleManager.getSampleNum()];

    int totalVariant = 0;
    int totalSnv = 0;
    int totalIndel = 0;
    // a b c d will be passed to calculated fisher p
    int qualifiedCase = 0;  // a
    int unqualifiedCase = 0; // b
    int qualifiedCtrl = 0;   // c
    int unqualifiedCtrl = 0; // d 
    float qualifiedCaseFreq = Data.FLOAT_NA;
    float qualifiedCtrlFreq = Data.FLOAT_NA;
    String enrichedDirection = Data.STRING_NA;
    double fetP = Data.DOUBLE_NA;
    double logisticP = Data.DOUBLE_NA;
    double linearP = Data.DOUBLE_NA;

    // no output
    static final int totalCase = SampleManager.getCaseNum();
    static final int totalCtrl = SampleManager.getCtrlNum();

    public CollapsingSummary(String name) {
        this.name = name;
    }

    public void setLogisticP(double value) {
        logisticP = value;
    }

    public void setLinearP(double value) {
        linearP = value;
    }

    public void updateSampleVariantCount4SingleVar(int index) {
        variantNumBySample[index] = variantNumBySample[index] + 1;
    }

    public void updateSampleVariantCount4CompHet(int index) {
        if (variantNumBySample[index] == 0) {
            variantNumBySample[index] = variantNumBySample[index] + 1;
        }
    }

    public void updateVariantCount(CollapsingOutput output) {
        totalVariant++;

        if (output.getCalledVar().isSNV()) {
            totalSnv++;
        } else {
            totalIndel++;
        }
    }

    public void countSample() {
        int s = 0;
        for (Sample sample : SampleManager.getSampleListBroadcast().value()) {
            if (sample.isCase()) {
                if (variantNumBySample[s] > 0) {
                    qualifiedCase++;
                }
            } else if (variantNumBySample[s] > 0) {
                qualifiedCtrl++;
            }
        }

        unqualifiedCase = totalCase - qualifiedCase;
        unqualifiedCtrl = totalCtrl - qualifiedCtrl;

        qualifiedCaseFreq = MathManager.devide(qualifiedCase, totalCase);
        qualifiedCtrlFreq = MathManager.devide(qualifiedCtrl, totalCtrl);

        if (qualifiedCaseFreq == 0
                && qualifiedCtrlFreq == 0) {
            enrichedDirection = Data.STRING_NA;
        } else if (qualifiedCaseFreq == qualifiedCtrlFreq) {
            enrichedDirection = "none";
        } else if (qualifiedCaseFreq < qualifiedCtrlFreq) {
            enrichedDirection = "ctrl";
        } else if (qualifiedCaseFreq > qualifiedCtrlFreq) {
            enrichedDirection = "case";
        }
    }

    public void calculateFetP() {
        fetP = FisherExact.getTwoTailedP(qualifiedCase, unqualifiedCase,
                qualifiedCtrl, unqualifiedCtrl);
    }

    public double getFetP() {
        return fetP;
    }

    @Override
    public int compareTo(Object another) throws ClassCastException {
        CollapsingSummary that = (CollapsingSummary) another;
        return Double.compare(this.fetP, that.fetP); //small -> large
    }
}
