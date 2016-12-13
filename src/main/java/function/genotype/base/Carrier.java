package function.genotype.base;

import global.Data;
import java.math.BigDecimal;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 * @author felipe
 */
public class Carrier extends NonCarrier {
    
    private int gatkFilteredCoverage;
    private int readsRef;
    private int readsAlt;
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
    
    public Carrier() {   
    }
    
    public Carrier(Row r) {
        sampleId = r.getInt(r.fieldIndex("sample_id"));        
        coverage = r.getInt(r.fieldIndex("samtools_raw_coverage"));
        genotype = r.getInt(r.fieldIndex("genotype"));
        gatkFilteredCoverage = r.getInt(r.fieldIndex("gatk_filtered_coverage"));
        readsRef = r.getInt(r.fieldIndex("reads_ref"));
        readsAlt = r.getInt(r.fieldIndex("reads_alt"));
        vqslod = getFloat((Float) r.get(r.fieldIndex("vqslod")));
        genotypeQualGQ = getFloat((Double) r.get(r.fieldIndex("genotype_qual_GQ")));
        strandBiasFS = getFloat((Double) r.get(r.fieldIndex("strand_bias_FS")));
        haplotypeScore = getFloat((Double) r.get(r.fieldIndex("haplotype_score")));
        rmsMapQualMQ = getFloat((Double) r.get(r.fieldIndex("rms_map_qual_MQ")));
        qualByDepthQD = getFloat((Double) r.get(r.fieldIndex("qual_by_depth_QD")));
        qual = getFloat(r.getDecimal(r.fieldIndex("qual")));
        readPosRankSum = getFloat((Double) r.get(r.fieldIndex("read_pos_rank_sum")));
        mapQualRankSum = getFloat((Double) r.get(r.fieldIndex("map_qual_rank_sum")));
        passFailStatus = r.getString(r.fieldIndex("pass_fail_status"));
        samplePheno = r.getShort(r.fieldIndex("pheno"));
    }
    
    public Carrier(Row r, int pheno) {
        sampleId = r.getInt(r.fieldIndex("sample_id"));        
        coverage = r.getInt(r.fieldIndex("samtools_raw_coverage"));
        genotype = r.getInt(r.fieldIndex("genotype"));
        gatkFilteredCoverage = r.getInt(r.fieldIndex("gatk_filtered_coverage"));
        readsRef = r.getInt(r.fieldIndex("reads_ref"));
        readsAlt = r.getInt(r.fieldIndex("reads_alt"));
        vqslod = getFloat((Double) r.get(r.fieldIndex("vqslod")));
        genotypeQualGQ = getFloat((Double) r.get(r.fieldIndex("genotype_qual_GQ")));
        strandBiasFS = getFloat((Double) r.get(r.fieldIndex("strand_bias_FS")));
        haplotypeScore = getFloat((Double) r.get(r.fieldIndex("haplotype_score")));
        rmsMapQualMQ = getFloat((Double) r.get(r.fieldIndex("rms_map_qual_MQ")));
        qualByDepthQD = getFloat((Double) r.get(r.fieldIndex("qual_by_depth_QD")));
        qual = getFloat(r.getDecimal(r.fieldIndex("qual")));
        readPosRankSum = getFloat((Double) r.get(r.fieldIndex("read_pos_rank_sum")));
        mapQualRankSum = getFloat((Double) r.get(r.fieldIndex("map_qual_rank_sum")));
        passFailStatus = r.getString(r.fieldIndex("pass_fail_status"));
        samplePheno = pheno;
    }
    
    private float getFloat(Float f) {
        if (f == null) {
            return Data.NA;
        }

        return f;
    }
    
    private float getFloat(Double f) {
        if (f == null) {
            return Data.NA;
        }

        return f.floatValue();
    }
    
    private float getFloat(BigDecimal f) {
        if (f == null) {
            return Data.NA;
        }

        return f.floatValue();
    }
    
    /* Copied from ATAV code */
    public int getGatkFilteredCoverage() {
        return gatkFilteredCoverage;
    }

    public int getReadsRef() {
        return readsRef;
    }

    public int getReadsAlt() {
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
    /* ---------- */
    
}
