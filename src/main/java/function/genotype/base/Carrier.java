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

    private int gatkFilteredCoverage;
    private short readsRef;
    private short readsAlt;
    private float vqslod;
    private float genotypeQualGQ;
    private float strandBiasFS;
    private float haplotypeScore;
    private float rmsMapQualMQ;
    private float qualByDepthQD;
    private float qual;
    private float readPosRankSum;
    private float mapQualRankSum;
    private String passFailStatus;

    public Carrier(Row r, byte pheno) {
        sampleId = r.getInt(r.fieldIndex("sample_id"));
        coverage = r.getInt(r.fieldIndex("samtools_raw_coverage"));
        genotype = r.getByte(r.fieldIndex("genotype"));
        gatkFilteredCoverage = r.getInt(r.fieldIndex("gatk_filtered_coverage"));
        readsRef = r.getShort(r.fieldIndex("reads_ref"));
        readsAlt = r.getShort(r.fieldIndex("reads_alt"));
        vqslod = getFloat((Float) r.get(r.fieldIndex("vqslod")));
        genotypeQualGQ = getFloat((Float) r.get(r.fieldIndex("genotype_qual_GQ")));
        strandBiasFS = getFloat((Float) r.get(r.fieldIndex("strand_bias_FS")));
        haplotypeScore = getFloat((Float) r.get(r.fieldIndex("haplotype_score")));
        rmsMapQualMQ = getFloat((Float) r.get(r.fieldIndex("rms_map_qual_MQ")));
        qualByDepthQD = getFloat((Float) r.get(r.fieldIndex("qual_by_depth_QD")));
        qual = getFloat((Float) r.get(r.fieldIndex("qual")));
        readPosRankSum = getFloat((Float) r.get(r.fieldIndex("read_pos_rank_sum")));
        mapQualRankSum = getFloat((Float) r.get(r.fieldIndex("map_qual_rank_sum")));
        passFailStatus = r.getString(r.fieldIndex("pass_fail_status"));
        samplePheno = pheno;
    }

    private float getFloat(Float f) {
        if (f == null) {
            return Data.FLOAT_NA;
        }

        return f;
    }

    public String getPercAltRead() {
        return FormatManager.getFloat(MathManager.devide(readsAlt, gatkFilteredCoverage));
    }

    public int getGatkFilteredCoverage() {
        return gatkFilteredCoverage;
    }

    public short getReadsRef() {
        return readsRef;
    }

    public short getReadsAlt() {
        return readsAlt;
    }

    public float getVqslod() {
        return vqslod;
    }

    public float getGenotypeQualGQ() {
        return genotypeQualGQ;
    }

    public float getStrandBiasFS() {
        return strandBiasFS;
    }

    public float getHaplotypeScore() {
        return haplotypeScore;
    }

    public float getRmsMapQualMQ() {
        return rmsMapQualMQ;
    }

    public float getQualByDepthQD() {
        return qualByDepthQD;
    }

    public float getQual() {
        return qual;
    }

    public float getReadPosRankSum() {
        return readPosRankSum;
    }

    public float getMapQualRankSum() {
        return mapQualRankSum;
    }

    public String getPassFailStatus() {
        return passFailStatus;
    }
}
