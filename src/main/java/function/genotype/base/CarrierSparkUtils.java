package function.genotype.base;

import global.Data;
import global.Index;
import java.util.LinkedList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 *
 * @author felipe
 */
public class CarrierSparkUtils {
    
    
    public static final String COVERAGE_COL = "samtools_raw_coverage";
    
    public static Dataset<Row> applyCarrierFilters(Dataset<Row> carrierDF) {
        LinkedList<Column> l = new LinkedList<>();
        
        
//        if(GenotypeLevelFilterCommand.minCaseCoverageCall != Data.NO_FILTER &&
//                GenotypeLevelFilterCommand.minCtrlCoverageCall != Data.NO_FILTER) {
//            l.add(lit(false)
//                    .or(col("pheno").equalTo(Index.CASE).and(col(COVERAGE_COL).geq(lit(GenotypeLevelFilterCommand.minCaseCoverageCall)) ))
//                    .or(col("pheno").equalTo(Index.CTRL).and(col(COVERAGE_COL).geq(lit(GenotypeLevelFilterCommand.minCtrlCoverageCall)) ))
//            );
//        } else if ( GenotypeLevelFilterCommand.minCaseCoverageCall != Data.NO_FILTER ) {
//            l.add(lit(false)
//                    .or(col("pheno").equalTo(Index.CASE).and(col(COVERAGE_COL).geq(lit(GenotypeLevelFilterCommand.minCaseCoverageCall)) ))
//                    .or(col("pheno").equalTo(Index.CTRL) )
//            );
//            
//        } else if ( GenotypeLevelFilterCommand.minCtrlCoverageCall != Data.NO_FILTER ) {
//            l.add(lit(false)
//                    .or(col("pheno").equalTo(Index.CASE) )
//                    .or(col("pheno").equalTo(Index.CTRL).and(col(COVERAGE_COL).geq(lit(GenotypeLevelFilterCommand.minCtrlCoverageCall)) ))
//            );
//        }
        
        if( GenotypeLevelFilterCommand.varStatus != null ) {
            Column c = col("pass_fail_status").isin((Object[])GenotypeLevelFilterCommand.varStatus);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }    
        if( GenotypeLevelFilterCommand.genotypeQualGQ != Data.NO_FILTER ) {
            Column c = col("genotype_qual_GQ").geq(GenotypeLevelFilterCommand.genotypeQualGQ);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.strandBiasFS != Data.NO_FILTER ) {
            Column c = col("strand_bias_FS").leq(GenotypeLevelFilterCommand.strandBiasFS);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.haplotypeScore != Data.NO_FILTER ) {
            Column c = col("haplotype_score").leq(GenotypeLevelFilterCommand.haplotypeScore);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.rmsMapQualMQ != Data.NO_FILTER ) {
            Column c = col("rms_map_qual_MQ").geq(GenotypeLevelFilterCommand.rmsMapQualMQ);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.qualByDepthQD != Data.NO_FILTER ) {
            Column c = col("qual_by_depth_QD").geq(GenotypeLevelFilterCommand.qualByDepthQD);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.qual != Data.NO_FILTER ) {
            Column c = col("qual").geq(GenotypeLevelFilterCommand.qual);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.readPosRankSum != Data.NO_FILTER ) {
            Column c = col("read_pos_rank_sum").geq(GenotypeLevelFilterCommand.readPosRankSum);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        if( GenotypeLevelFilterCommand.mapQualRankSum != Data.NO_FILTER ) {
            Column c = col("map_qual_rank_sum").geq(GenotypeLevelFilterCommand.mapQualRankSum);
            if(GenotypeLevelFilterCommand.isQcMissingIncluded)
                l.add(col("pass_fail_status").isNull().or(c));
            else
                l.add(c);
        }
        
        
        
            
        
        // if there is some filter to be applied, build where clause
        if(l.size() > 0) {
            Column whereCondition = l.pop();
            while(l.size() > 0)
                whereCondition = whereCondition.and(l.pop());
            return carrierDF.withColumn(COVERAGE_COL,
                    when(whereCondition, col(COVERAGE_COL))
                            .otherwise(lit((short) Data.NA)) );
        }
        
        // otherwise, return the same outputDF
        return carrierDF;
    }
    
}
