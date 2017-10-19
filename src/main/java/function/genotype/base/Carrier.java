package function.genotype.base;

import global.Data;
import org.apache.spark.sql.Row;
import utils.FormatManager;
import utils.MathManager;

/**
 *
 * @author nick
 */
public class Carrier extends NonCarrier {

    private short adREF;
    private short adAlt;
    private int GQ;
    private float vqslod;
    private float FS;
    private short MQ;
    private short QD;
    private int qual;
    private float readPosRankSum;
    private float mapQualRankSum;
    private String FILTER;

    public Carrier(Row r, byte pheno) {
        sampleId = r.getInt(r.fieldIndex("sample_id"));
        GT = r.getByte(r.fieldIndex("GT"));
        DPbin = r.getShort(r.fieldIndex("DP"));
        adREF = getShort((Short) r.get(r.fieldIndex("AD_REF")));
        adAlt = getShort((Short) r.get(r.fieldIndex("AD_ALT")));
        GQ = r.getInt(r.fieldIndex("GQ"));
        vqslod = getFloat((Float) r.get(r.fieldIndex("VQSLOD")));
        FS = getFloat((Float) r.get(r.fieldIndex("FS")));
        MQ = getShort((Short) r.get(r.fieldIndex("MQ")));
        QD = getShort((Short) r.get(r.fieldIndex("QD")));
        qual = getInt((Integer) r.get(r.fieldIndex("QUAL")));
        readPosRankSum = getFloat((Float) r.get(r.fieldIndex("ReadPosRankSum")));
        mapQualRankSum = getFloat((Float) r.get(r.fieldIndex("MQRankSum")));
        FILTER = r.getString(r.fieldIndex("FILTER"));
        samplePheno = pheno;
    }

    private float getFloat(Float f) {
        if (f == null) {
            return Data.FLOAT_NA;
        }

        return f;
    }

    private short getShort(Short s) {
        if (s == null) {
            return Data.SHORT_NA;
        }

        return s;
    }
    
    private int getInt(Integer i) {
        if (i == null) {
            return Data.INTEGER_NA;
        }

        return i;
    }

    public String getPercAltRead() {
        return FormatManager.getFloat(MathManager.devide(adAlt, DPbin));
    }

    public short getADRef() {
        return adREF;
    }

    public short getADAlt() {
        return adAlt;
    }

    public float getVqslod() {
        return vqslod;
    }

    public int getGQ() {
        return GQ;
    }

    public float getFS() {
        return FS;
    }

    public short getMQ() {
        return MQ;
    }

    public short getQD() {
        return QD;
    }

    public int getQual() {
        return qual;
    }

    public float getReadPosRankSum() {
        return readPosRankSum;
    }

    public float getMapQualRankSum() {
        return mapQualRankSum;
    }

    public String getPassFailStatus() {
        return FILTER;
    }
}
