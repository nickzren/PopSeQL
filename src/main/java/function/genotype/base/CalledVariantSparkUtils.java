package function.genotype.base;

import global.Data;
import global.Index;
import global.PopSpark;
import global.Utils;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
//import static org.apache.spark.sql.functions.monotonicallyIncreasingId;

/**
 *
 * @author felipe
 */
public class CalledVariantSparkUtils {
    
    public static int[] covCallFilter;
    public static int[] covNoCallFilter;
    
    public static void initCovFilters(Broadcast<int[]> minCovCallBr, Broadcast<int[]> minCovNoCallBr) {
        covCallFilter = minCovCallBr.value();
        covNoCallFilter = minCovNoCallBr.value();
    }

    public static Dataset<Row> getCalledVariantDF() {
        
        // Uncomment this to read called_variant table from JDBC
//        return PopSpark.session.read().jdbc(PopSpark.jdbcURL,
//                "( select * from called_variant ) t1",
//                new Properties());

        // Set called_variant parquet filepath here
        return PopSpark.session.read().parquet("/popseql/igm_ctrl/called_variant/part*");
    }
    
    public static Dataset<Row> getReadCoverageDF(String[] blockIds) {
        
        // Uncomment this to read read_coverage table from JDBC
//        return PopSpark.session.read().jdbc(PopSpark.jdbcURL,
//                        "( select * from read_coverage ) t1",
//                        new Properties());

        // Set read_coverage parquet filepath here
        return PopSpark.session.read().parquet("/popseql/igm_ctrl/read_coverage/part*");
    }

    public static Dataset<Row> getSampleIdDF(Dataset<Row> cvDF) {
        return PopSpark.session.read().jdbc(PopSpark.jdbcURL, "( select distinct sample_id from read_coverage ) t1", new Properties()).select("sample_id").distinct();
    }

    public static Dataset<Row> getVarChrPosDF(Dataset<Row> cvDF) {
        return cvDF.select("chr", "pos", "block_id", "variant_id").distinct();
    }

    public static Dataset<Row> getBlockIdDF(Dataset<Row> cvDF) {
        return cvDF.select("block_id").distinct();
    }
    
    public static Dataset<Row> applyOutputFilters(Dataset<Row> outputDF) {
        LinkedList<Column> l = new LinkedList<>();
        if(GenotypeLevelFilterCommand.minCtrlMaf != Data.NO_FILTER) {
            l.add(col("Ctrl Maf").geq(GenotypeLevelFilterCommand.minCtrlMaf));
        }
        if(GenotypeLevelFilterCommand.maxCtrlMaf != Data.NO_FILTER) {
            l.add(col("Ctrl Maf").leq(GenotypeLevelFilterCommand.maxCtrlMaf));
            System.out.print("filter: ");
            System.out.println(GenotypeLevelFilterCommand.maxCtrlMaf);
        }
        if(GenotypeLevelFilterCommand.maxQcFailSample != Data.NO_FILTER)
            l.add(col("QC Fail Case").plus(col("QC Fail Ctrl")).leq(GenotypeLevelFilterCommand.maxQcFailSample));
        
        if(GenotypeLevelFilterCommand.minCaseCarrier != Data.NO_FILTER)
            l.add(col("case_carrier").geq(GenotypeLevelFilterCommand.minCaseCarrier));
        if(GenotypeLevelFilterCommand.minVarPresent != Data.NO_FILTER)
            l.add(col("var_present").geq(GenotypeLevelFilterCommand.minVarPresent));
        
        // if there is some filter to be applied, build where clause
        if(l.size() > 0) {
            System.out.println("\t> Applying output filters");
            Column whereCondition = l.pop();
            while(l.size() > 0)
                whereCondition = whereCondition.and(l.pop());
            return outputDF.where(whereCondition);
        }
        
        // otherwise, return the same outputDF
        return outputDF;
    }
    
    //    public static Dataset<Row> getCarrierDF( Dataset<Row> posSampleCrossDF, Dataset<Row> cvDF) {
//        return posSampleCrossDF.join(cvDF,
//                    cvDF.col("sample_id").equalTo(posSampleCrossDF.col("p_sample_id"))
//                    .and(cvDF.col("chr").equalTo(posSampleCrossDF.col("p_chr")))
//                    .and(cvDF.col("pos").equalTo(posSampleCrossDF.col("p_pos"))),
//            "left");
//    }
    
}
