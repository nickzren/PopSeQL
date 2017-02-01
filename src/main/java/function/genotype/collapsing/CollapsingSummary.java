package function.genotype.collapsing;

import function.genotype.base.SampleManager;
import function.genotype.statistics.FisherExact;
import global.Data;
import global.Index;
import java.util.HashMap;
import java.util.Map.Entry;
import utils.MathManager;

/**
 *
 * @author nick
 */
public class CollapsingSummary implements Comparable {

    String name; // gene name or region name

    HashMap<Integer, Integer> sampleVariantCountMap = new HashMap<>();

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

    public void updateSampleVariantCount4SingleVar(int sampleId) {
        Integer variantCount = sampleVariantCountMap.get(sampleId);

        if (variantCount == null) {
            variantCount = 0;
        }

        variantCount++;
        sampleVariantCountMap.put(sampleId, variantCount);
    }

    public void updateVariantCount(CollapsingOutput output) {
        totalVariant++;

        if (output.getCalledVar().isSNV()) {
            totalSnv++;
        } else {
            totalIndel++;
        }
    }

    public void countSample(HashMap<Integer, Byte> sampleMap) {
        for (Entry<Integer, Byte> entry : sampleMap.entrySet()) {
            if (sampleVariantCountMap.get(entry.getKey()) > 0) {
                if (entry.getValue() == Index.CASE) {
                    qualifiedCase++;
                } else {
                    qualifiedCtrl++;
                }
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
