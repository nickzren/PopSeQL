package function.genotype.collapsing;

import function.genotype.base.SampleManager;
import function.genotype.statistics.FisherExact;
import global.Data;
import global.Index;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.spark.Accumulator;
import utils.MathManager;
import utils.SparkManager;

/**
 *
 * @author nick
 */
public class CollapsingSummary implements Comparable, Serializable {

    String name; // gene name or region name

    HashMap<Integer, Accumulator<Integer>> sampleVariantCountMap = new HashMap<>();

    Accumulator<Integer> totalVariant;
    Accumulator<Integer> totalSnv;
    Accumulator<Integer> totalIndel;

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

        SampleManager.getSamplePhenoMap().entrySet().stream().forEach((entry) -> {
            sampleVariantCountMap.put(entry.getKey(), SparkManager.context.accumulator(0));
        });

        totalVariant = SparkManager.context.accumulator(0);
        totalSnv = SparkManager.context.accumulator(0);
        totalIndel = SparkManager.context.accumulator(0);
    }

    public void setLogisticP(double value) {
        logisticP = value;
    }

    public void setLinearP(double value) {
        linearP = value;
    }

    public void updateSampleVariantCount4SingleVar(int sampleId) {
        sampleVariantCountMap.get(sampleId).add(1);
    }

    public void updateVariantCount(CollapsingOutput output) {
        totalVariant.add(1);

        if (output.getCalledVar().isSNV()) {
            totalSnv.add(1);
        } else {
            totalIndel.add(1);
        }
    }

    public void countSample() {
        for (Entry<Integer, Byte> entry : SampleManager.getSamplePhenoMap().entrySet()) {
            if (sampleVariantCountMap.get(entry.getKey()).value() > 0) {
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
