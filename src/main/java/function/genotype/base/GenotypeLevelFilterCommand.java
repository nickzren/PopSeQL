package function.genotype.base;

import global.Data;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static utils.CommandManager.getValidDouble;
import static utils.CommandManager.getValidFloat;
import static utils.CommandManager.getValidInteger;
import utils.CommandOption;
import utils.SparkManager;
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
    public static String[] varStatus; // null: no filer or all    
    public static float genotypeQualGQ = Data.NO_FILTER;
    public static float strandBiasFS = Data.NO_FILTER;
    public static float haplotypeScore = Data.NO_FILTER;
    public static float rmsMapQualMQ = Data.NO_FILTER;
    public static float qualByDepthQD = Data.NO_FILTER;
    public static float qual = Data.NO_FILTER;
    public static float readPosRankSum = Data.NO_FILTER;
    public static float mapQualRankSum = Data.NO_FILTER;
    public static boolean isQcMissingIncluded = false;
    public static int maxQcFailSample = Data.NO_FILTER;
    public static String calledVariantDataPath = ""; // input data format required parquet format
    public static String readCoverageDataPath = "";  // input data format required parquet format

    public static final String[] VARIANT_STATUS = {"pass", "pass+intermediate", "all"};

    public static void initOptions(Iterator<CommandOption> iterator)
            throws Exception {
        CommandOption option;

        while (iterator.hasNext()) {
            option = (CommandOption) iterator.next();
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
                case "--var-status":
                    checkValueValid(VARIANT_STATUS, option);
                    String str = option.getValue().replace("+", ",");
                    if (str.contains("all")) {
                        varStatus = null;
                    } else {
                        varStatus = str.split(",");
                    }
                    break;
                case "--gq":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    genotypeQualGQ = getValidFloat(option);
                    break;
                case "--fs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    strandBiasFS = getValidFloat(option);
                    break;
                case "--hap-score":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    haplotypeScore = getValidFloat(option);
                    break;
                case "--mq":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    rmsMapQualMQ = getValidFloat(option);
                    break;
                case "--qd":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    qualByDepthQD = getValidFloat(option);
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
        return SparkManager.session.read().parquet(calledVariantDataPath);
    }

    public static Dataset<Row> getReadCoverageDF() {
        return SparkManager.session.read().parquet(readCoverageDataPath);
    }

    public static Dataset<Row> applyCarrierFilters(Dataset<Row> carrierDF) {
        LinkedList<Column> l = new LinkedList<>();

        if (varStatus != null) {
            Column c = col("pass_fail_status").isin((Object[]) varStatus);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (genotypeQualGQ != Data.NO_FILTER) {
            Column c = col("genotype_qual_GQ").geq(genotypeQualGQ);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (strandBiasFS != Data.NO_FILTER) {
            Column c = col("strand_bias_FS").leq(strandBiasFS);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (haplotypeScore != Data.NO_FILTER) {
            Column c = col("haplotype_score").leq(haplotypeScore);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (rmsMapQualMQ != Data.NO_FILTER) {
            Column c = col("rms_map_qual_MQ").geq(rmsMapQualMQ);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (qualByDepthQD != Data.NO_FILTER) {
            Column c = col("qual_by_depth_QD").geq(qualByDepthQD);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (qual != Data.NO_FILTER) {
            Column c = col("qual").geq(qual);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (readPosRankSum != Data.NO_FILTER) {
            Column c = col("read_pos_rank_sum").geq(readPosRankSum);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
            } else {
                l.add(c);
            }
        }
        if (mapQualRankSum != Data.NO_FILTER) {
            Column c = col("map_qual_rank_sum").geq(mapQualRankSum);
            if (isQcMissingIncluded) {
                l.add(col("pass_fail_status").isNull().or(c));
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
            return carrierDF.withColumn("samtools_raw_coverage",
                    when(whereCondition, col("samtools_raw_coverage"))
                    .otherwise(lit((short) Data.SHORT_NA)));
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
}
