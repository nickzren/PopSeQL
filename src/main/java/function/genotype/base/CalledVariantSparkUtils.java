package function.genotype.base;

import global.Data;
import utils.PopSpark;
import java.util.LinkedList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author felipe, nick
 */
public class CalledVariantSparkUtils {

    public static int[] covCallFilter;
    public static int[] covNoCallFilter;

    public static void initCovFilters() {
        covCallFilter = GenotypeLevelFilterCommand.getCovCallFiltersBroadcast().value();
        covNoCallFilter = GenotypeLevelFilterCommand.getCovNoCallFiltersBroadcast().value();
    }

    public static Dataset<Row> getCalledVariantDF() {
        // Set called_variant parquet filepath here
         return PopSpark.session.read().parquet("/custom_capture/parquet/called_variant/part*");
//        return PopSpark.session.read().parquet("file:///Users/zr2180/Desktop/data/newdb/parquet/called_variant/part*");
    }

    public static Dataset<Row> getReadCoverageDF() {
        // Set read_coverage parquet filepath here
        return PopSpark.session.read().parquet("/custom_capture/parquet/read_coverage/part*");
//        return PopSpark.session.read().parquet("file:///Users/zr2180/Desktop/data/newdb/parquet/read_coverage/part*");
    }

    public static Dataset<Row> applyOutputFilters(Dataset<Row> outputDF) {
        LinkedList<Column> l = new LinkedList<>();
        if (GenotypeLevelFilterCommand.minCtrlMaf != Data.NO_FILTER) {
            l.add(col("Ctrl Maf").geq(GenotypeLevelFilterCommand.minCtrlMaf));
        }
        if (GenotypeLevelFilterCommand.maxCtrlMaf != Data.NO_FILTER) {
            l.add(col("Ctrl Maf").leq(GenotypeLevelFilterCommand.maxCtrlMaf));
        }
        if (GenotypeLevelFilterCommand.maxQcFailSample != Data.NO_FILTER) {
            l.add(col("QC Fail Case").plus(col("QC Fail Ctrl")).leq(GenotypeLevelFilterCommand.maxQcFailSample));
        }
        if (GenotypeLevelFilterCommand.minCaseCarrier != Data.NO_FILTER) {
            l.add(col("case_carrier").geq(GenotypeLevelFilterCommand.minCaseCarrier));
        }
        if (GenotypeLevelFilterCommand.minVarPresent != Data.NO_FILTER) {
            l.add(col("var_present").geq(GenotypeLevelFilterCommand.minVarPresent));
        }

        // if there is some filter to be applied, build where clause
        if (l.size() > 0) {
            System.out.println("\t> Applying output filters");
            Column whereCondition = l.pop();
            while (l.size() > 0) {
                whereCondition = whereCondition.and(l.pop());
            }
            return outputDF.where(whereCondition);
        }

        // otherwise, return the same outputDF
        return outputDF;
    }
}
