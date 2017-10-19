package function.genotype.base;

import global.Data;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static utils.CommandManager.getValidDouble;
import static utils.CommandManager.getValidFloat;
import static utils.CommandManager.getValidInteger;
import utils.CommandOption;
import utils.SparkManager;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.checkValueValid;

/**
 *
 * @author nick
 */
public class GenotypeLevelFilterCommand {

    public static String sampleFile = "";
    public static double maxCtrlMaf = Data.NO_FILTER;
    public static double minCtrlMaf = Data.NO_FILTER;
    public static int minCoverage = Data.NO_FILTER;
    public static int minCaseCoverageCall = Data.NO_FILTER;
    public static int minCaseCoverageNoCall = Data.NO_FILTER;
    public static int minCtrlCoverageCall = Data.NO_FILTER;
    public static int minCtrlCoverageNoCall = Data.NO_FILTER;
    public static float GQ = Data.NO_FILTER;
    public static float FS = Data.NO_FILTER;
    public static float MQ = Data.NO_FILTER;
    public static float QD = Data.NO_FILTER;
    public static float qual = Data.NO_FILTER;
    public static float readPosRankSum = Data.NO_FILTER;
    public static float mapQualRankSum = Data.NO_FILTER;
    public static boolean isQcMissingIncluded = false;
    public static int maxQcFailSample = Data.NO_FILTER;
    public static int minVarPresent = 1; // special case
    public static String calledVariantDataPath = ""; // input data format required parquet format
    public static String readCoverageDataPath = "";  // input data format required parquet format

    public static final String[] VARIANT_STATUS = {"pass", "pass+intermediate", "all"};

    public static void initOptions(Iterator<CommandOption> iterator)
            throws Exception {
        CommandOption option;

        while (iterator.hasNext()) {
            option = iterator.next();
            switch (option.getName()) {
                case "--sample":
                    sampleFile = option.getValue();
                    break;
                case "--called-variant":
                    calledVariantDataPath = option.getValue();
                    break;
                case "--read-coverage":
                    readCoverageDataPath = option.getValue();
                    break;
                case "--ctrl-maf":
                    checkValueValid(0.5, 0, option);
                    maxCtrlMaf = getValidDouble(option);
                    break;
                case "--min-coverage":
                    checkValueValid(new String[]{"0", "3", "10", "20", "201"}, option);
                    minCoverage = getValidInteger(option);
                    break;
                case "--gq":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    GQ = getValidFloat(option);
                    break;
                case "--fs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    FS = getValidFloat(option);
                    break;
                case "--mq":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    MQ = getValidFloat(option);
                    break;
                case "--qd":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    QD = getValidFloat(option);
                    break;
                case "--qual":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    qual = getValidFloat(option);
                    break;
                case "--rprs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    readPosRankSum = getValidFloat(option);
                    break;
                case "--mqrs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    mapQualRankSum = getValidFloat(option);
                    break;
                case "--include-qc-missing":
                    isQcMissingIncluded = true;
                    break;
                case "--max-qc-fail-sample":
                    checkValueValid(Data.NO_FILTER, 0, option);
                    maxQcFailSample = getValidInteger(option);
                    break;
                default:
                    continue;
            }

            iterator.remove();
        }
    }

    public static Dataset<Row> getCalledVariantDF() {
//        return SparkManager.session.read()
//                .format("com.databricks.spark.csv")
//                //                .option("inferSchema", "true")
//                //                .option("header", "true")
//                .option("delimiter", "\t")
//                .option("nullValue", "\\N")
//                .load(calledVariantDataPath);

        return SparkManager.session.read().parquet(calledVariantDataPath);
    }

    public static Dataset<Row> getReadCoverageDF() {
//        return SparkManager.session.read()
//                .format("com.databricks.spark.csv")
//                //                .option("inferSchema", "true")
//                //                .option("header", "true")
//                .option("delimiter", "\t")
//                .option("nullValue", "\\N")
//                .load(readCoverageDataPath);

        return SparkManager.session.read().parquet(readCoverageDataPath);
    }

    public static Dataset<Row> applyCarrierFilters(Dataset<Row> carrierDF) {
        LinkedList<Column> l = new LinkedList<>();

        if (GQ != Data.NO_FILTER) {
            Column c = col("GQ").geq(GQ);
            if (isQcMissingIncluded) {
                l.add(col("GQ").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (FS != Data.NO_FILTER) {
            Column c = col("FS").leq(FS);
            if (isQcMissingIncluded) {
                l.add(col("FS").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (MQ != Data.NO_FILTER) {
            Column c = col("MQ").geq(MQ);
            if (isQcMissingIncluded) {
                l.add(col("MQ").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (QD != Data.NO_FILTER) {
            Column c = col("QD").geq(QD);
            if (isQcMissingIncluded) {
                l.add(col("QD").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (qual != Data.NO_FILTER) {
            Column c = col("QUAL").geq(qual);
            if (isQcMissingIncluded) {
                l.add(col("QUAL").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (readPosRankSum != Data.NO_FILTER) {
            Column c = col("ReadPosRankSum").geq(readPosRankSum);
            if (isQcMissingIncluded) {
                l.add(col("ReadPosRankSum").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (mapQualRankSum != Data.NO_FILTER) {
            Column c = col("MQRankSum").geq(mapQualRankSum);
            if (isQcMissingIncluded) {
                l.add(col("MQRankSum").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (minCoverage != Data.NO_FILTER) {
            Column c = col("DP").geq(minCoverage);
            if (isQcMissingIncluded) {
                l.add(col("DP").isNull().or(c));
            } else {
                l.add(c);
            }
        }

        // if there is some filter to be applied, build where clause
        if (l.size() > 0) {
//            Column whereCondition = l.pop();
            Column whereCondition = lit(true);
            while (l.size() > 0) {
                whereCondition = whereCondition.and(l.pop());
            }
            return carrierDF.where(whereCondition);
        }

        return carrierDF;
    }

    public static Dataset<Row> applyOutputFilters(Dataset<Row> outputDF) {
        LinkedList<Column> l = new LinkedList<>();
        if (minCtrlMaf != Data.NO_FILTER) {
            l.add(col("Ctrl Maf").geq(minCtrlMaf));
        }
        if (maxCtrlMaf != Data.NO_FILTER) {
            l.add(col("Ctrl Maf").leq(maxCtrlMaf));
        }
        if (maxQcFailSample != Data.NO_FILTER) {
            l.add(col("QC Fail Case").plus(col("QC Fail Ctrl")).leq(maxQcFailSample));
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

        return outputDF;
    }

    public static boolean isMaxCtrlMafValid(double value) {
        if (maxCtrlMaf == Data.NO_FILTER) {
            return true;
        }

        return value <= maxCtrlMaf;
    }

    public static boolean isMinVarPresentValid(int value) {
        if (minVarPresent == Data.NO_FILTER) {
            return true;
        }

        return value >= minVarPresent;
    }
}
