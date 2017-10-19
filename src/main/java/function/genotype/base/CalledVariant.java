package function.genotype.base;

import java.util.HashMap;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 */
public class CalledVariant {

    private HashMap<Integer, Carrier> carrierMap = new HashMap<>();
    private HashMap<Integer, NonCarrier> noncarrierMap = new HashMap<>();

    public int variantId;
    public String variantIdStr;
    public String allele;
    public String refAllele;
    public String chrStr;
    public int position;
    private boolean isIndel;

    public short blockOffset;

    public void addCarrier(int sampleId, Carrier carrier) {
        carrierMap.put(sampleId, carrier);
    }

    public void addNonCarrier(int sampleId, NonCarrier noncarrier) {
        noncarrierMap.put(sampleId, noncarrier);
    }

    public void initVariantData(Row r) {
        chrStr = r.getString(r.fieldIndex("chr"));
        position = r.getInt(r.fieldIndex("pos"));
        allele = r.getString(r.fieldIndex("alt"));
        refAllele = r.getString(r.fieldIndex("ref"));

        variantIdStr = chrStr + "-" + position + "-" + refAllele + "-" + allele;

        isIndel = allele.length() != refAllele.length();

        blockOffset = (short) (position % 1000); // 1k block size
    }

    public HashMap<Integer, Carrier> getCarrierMap() {
        return carrierMap;
    }

    public HashMap<Integer, NonCarrier> getNonCarrierMap() {
        return noncarrierMap;
    }

//    public byte getGT() {
//        
//    }

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

    public boolean isSNV() {
        return !isIndel;
    }

    public String getAllele() {
        return allele;
    }

    public String getRefAllele() {
        return refAllele;
    }
}
