package function.genotype.base;

import function.variant.base.Output;
import function.variant.base.Region;
import global.Data;
import java.util.HashMap;
import java.util.Collection;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 * @author felipe
 */
public class CalledVariant extends Region {

    private HashMap<Integer, Carrier> carrierMap = new HashMap<>();
    private HashMap<Integer, NonCarrier> noncarrierMap = new HashMap<>();
//    private int[] genotype = new int[SampleManager.getListSize()];
//    private int[] coverage = new int[SampleManager.getListSize()];
    private int[] qcFailSample = {0, 0};

    public int variantId;
    public String variantIdStr;
    public String allele;
    public String refAllele;
    private boolean isIndel;

    public short blockOffset;

    public void addCarrier(Row r, int sampleId, short pheno) {
        int coverage = r.getInt(r.fieldIndex("samtools_raw_coverage"));

        if (coverage == Data.NA) {
            qcFailSample[pheno]++;
            return;
        }

        if (CalledVariantSparkUtils.covCallFilter[pheno] != Data.NO_FILTER) {
            if (coverage < CalledVariantSparkUtils.covCallFilter[pheno]) {
                qcFailSample[pheno]++;
                return;
            }
        }

        carrierMap.put(sampleId, new Carrier(r, pheno));
    }

    public void addNonCarrier(int sampleId, short coverage, short pheno) {
        if (CalledVariantSparkUtils.covNoCallFilter[pheno] != Data.NO_FILTER) {
            if (coverage < CalledVariantSparkUtils.covNoCallFilter[pheno]) {
                return;
            }
        }

        noncarrierMap.put(sampleId, new NonCarrier(sampleId, coverage, pheno));
    }

    public void addSampleDataToOutput(Output output) {
        for (Carrier c : carrierMap.values()) {
            output.addSampleGeno(c.getGenotype(), c.getSamplePheno());
        }
        for (NonCarrier nc : noncarrierMap.values()) {
            output.addSampleGeno(nc.getGenotype(), nc.getSamplePheno());
        }
    }

    public void initVariantData(Row r) {
        chrStr = r.getString(r.fieldIndex("chr"));
        chrNum = intChr();

        allele = r.getString(r.fieldIndex("alt"));
        refAllele = r.getString(r.fieldIndex("ref"));

        int position = r.getInt(r.fieldIndex("pos"));

        variantIdStr
                = chrStr + "-" + Integer.toString(position) + "-"
                + refAllele + "-" + allele;

        isIndel = allele.length() != refAllele.length();

        // Magic trick to get block offset
        blockOffset = (short) ((position - 1) & 0x3FF);

        initRegion(chrStr, position, position);

    }

    public Collection<Carrier> getCarriers() {
        return carrierMap.values();
    }

    public Collection<NonCarrier> getNonCarriers() {
        return noncarrierMap.values();
    }

    private int intChr() {
        if (chrStr.equals("X")
                || chrStr.equals("XY")) {
            return 23;
        } else if (chrStr.equals("Y")) {
            return 24;
        } else if (chrStr.equals("MT")) {
            return 26;
        } else {
            try {
                return Integer.parseInt(chrStr);
            } catch (NumberFormatException e) {
                return Data.NA;
            }
        }
    }

    public String getVariantIdStr() {
        return variantIdStr;
    }

    public String getType() {
        if (isIndel) {
            return "indel";
        } else {
            return "snv";
        }
    }

    public String getAllele() {
        return allele;
    }

    public String getRefAllele() {
        return refAllele;
    }

    public int getQcFailSample(int pheno) {
        return qcFailSample[pheno];
    }
}
