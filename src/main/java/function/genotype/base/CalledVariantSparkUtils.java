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

    public static Dataset<Row> getCalledVariantDF() {
        return PopSpark.session.read().parquet(GenotypeLevelFilterCommand.calledVariantDataPath);
    }

    public static Dataset<Row> getReadCoverageDF() {
        return PopSpark.session.read().parquet(GenotypeLevelFilterCommand.readCoverageDataPath);
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
