package function.genotype.base;

import global.Data;
import global.Index;
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
public class NonCarrierSparkUtils {
    public static Dataset<Row> applyCoverageFiltersToNonCarriers(Dataset<Row> nonCarrierDF) {
        Column caseCondition =
                when(col("coverage").lt(lit(GenotypeLevelFilterCommand.minCaseCoverageNoCall)), (short) Data.NA)
                .otherwise(col("coverage"));
        Column ctrlCondition =
                when(col("coverage").lt(lit(GenotypeLevelFilterCommand.minCtrlCoverageNoCall)), (short) Data.NA)
                .otherwise(col("coverage"));
        
        if(GenotypeLevelFilterCommand.minCaseCoverageNoCall != Data.NO_FILTER &&
                GenotypeLevelFilterCommand.minCaseCoverageNoCall != Data.NO_FILTER) {
            return nonCarrierDF.withColumn("coverage",
                    when( col("pheno").equalTo(lit(Index.CTRL)), ctrlCondition)
                    .otherwise(caseCondition) );
        } else if ( GenotypeLevelFilterCommand.minCaseCoverageNoCall != Data.NO_FILTER ) {
            return nonCarrierDF.withColumn("coverage",
                    when( col("pheno").equalTo(lit(Index.CASE)), caseCondition)
                    .otherwise(lit("coverage")));
        } else if ( GenotypeLevelFilterCommand.minCtrlCoverageNoCall != Data.NO_FILTER ) {
            return nonCarrierDF.withColumn("coverage",
                    when( col("pheno").equalTo(lit(Index.CTRL)), ctrlCondition)
                    .otherwise(lit("coverage")));
        }
        return nonCarrierDF;
    }
}
