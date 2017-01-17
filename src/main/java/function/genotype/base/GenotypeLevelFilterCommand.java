package function.genotype.base;

import global.Data;
import utils.PopSpark;
import java.util.Iterator;
import org.apache.spark.broadcast.Broadcast;
import static utils.CommandManager.checkRangeValid;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.getValidDouble;
import static utils.CommandManager.getValidInteger;
import static utils.CommandManager.getValidPath;
import static utils.CommandManager.getValidRange;
import utils.CommandOption;

/**
 *
 * @author nick
 */
public class GenotypeLevelFilterCommand {

    public static String sampleFile = "";
    public static boolean isAllSample = false;
    public static boolean isAllNonRef = false;
    public static boolean isAllGeno = false;
    public static double maxCtrlMaf = Data.NO_FILTER;
    public static double minCtrlMaf = Data.NO_FILTER;
    public static int minCoverage = Data.NO_FILTER;
    public static int minCaseCoverageCall = Data.NO_FILTER;
    public static int minCaseCoverageNoCall = Data.NO_FILTER;
    public static int minCtrlCoverageCall = Data.NO_FILTER;
    public static int minCtrlCoverageNoCall = Data.NO_FILTER;
    public static int minVarPresent = 1; // special case
    public static int minCaseCarrier = Data.NO_FILTER;
    public static String[] varStatus; // null: no filer or all    
    public static double[] hetPercentAltRead = null; // {min, max}
    public static double[] homPercentAltRead = null;
    public static double genotypeQualGQ = Data.NO_FILTER;
    public static double strandBiasFS = Data.NO_FILTER;
    public static double haplotypeScore = Data.NO_FILTER;
    public static double rmsMapQualMQ = Data.NO_FILTER;
    public static double qualByDepthQD = Data.NO_FILTER;
    public static double qual = Data.NO_FILTER;
    public static double readPosRankSum = Data.NO_FILTER;
    public static double mapQualRankSum = Data.NO_FILTER;
    public static boolean isQcMissingIncluded = false;
    public static int maxQcFailSample = Data.NO_FILTER;

    public static Broadcast<int[]> covCallFiltersBroadcast;
    public static Broadcast<int[]> covNoCallFiltersBroadcast;

    public static final String[] VARIANT_STATUS = {"pass", "pass+intermediate", "all"};

    public static void initOptions(Iterator<CommandOption> iterator)
            throws Exception {
        CommandOption option;

        while (iterator.hasNext()) {
            option = (CommandOption) iterator.next();
            switch (option.getName()) {
                case "--sample":
                case "--pedinfo":
                    sampleFile = getValidPath(option);
                    break;
                case "--all-sample":
                    isAllSample = true;
                    break;
                case "--all-non-ref":
                    isAllNonRef = true;
                    break;
                case "--all-geno":
                    isAllGeno = true;
                    break;
                case "--ctrlMAF":
                case "--ctrl-maf":
                case "--max-ctrl-maf":
                    checkValueValid(0.5, 0, option);
                    maxCtrlMaf = getValidDouble(option);
                    break;
                case "--min-ctrl-maf":
                    checkValueValid(0.5, 0, option);
                    minCtrlMaf = getValidDouble(option);
                    break;
                case "--min-coverage":
                    checkValueValid(new String[]{"0", "3", "10", "20", "201"}, option);
                    minCoverage = getValidInteger(option);
                    break;
                case "--min-case-coverage-call":
                    checkValueValid(Data.NO_FILTER, 0, option);
                    minCaseCoverageCall = getValidInteger(option);
                    break;
                case "--min-case-coverage-no-call":
                    checkValueValid(new String[]{"3", "10", "20", "201"}, option);
                    minCaseCoverageNoCall = getValidInteger(option);
                    break;
                case "--min-ctrl-coverage-call":
                    checkValueValid(Data.NO_FILTER, 0, option);
                    minCtrlCoverageCall = getValidInteger(option);
                    break;
                case "--min-ctrl-coverage-no-call":
                    checkValueValid(new String[]{"3", "10", "20", "201"}, option);
                    minCtrlCoverageNoCall = getValidInteger(option);
                    break;
                case "--min-variant-present":
                    checkValueValid(Data.NO_FILTER, 0, option);
                    minVarPresent = getValidInteger(option);
                    break;
                case "--min-case-carrier":
                    checkValueValid(Data.NO_FILTER, 0, option);
                    minCaseCarrier = getValidInteger(option);
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
                case "--het-percent-alt-read":
                    checkRangeValid("0-1", option);
                    hetPercentAltRead = getValidRange(option);
                    break;
                case "--hom-percent-alt-read":
                    checkRangeValid("0-1", option);
                    homPercentAltRead = getValidRange(option);
                    break;
                case "--gq":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    genotypeQualGQ = getValidDouble(option);
                    break;
                case "--fs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    strandBiasFS = getValidDouble(option);
                    break;
                case "--hap-score":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    haplotypeScore = getValidDouble(option);
                    break;
                case "--mq":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    rmsMapQualMQ = getValidDouble(option);
                    break;
                case "--qd":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    qualByDepthQD = getValidDouble(option);
                    break;
                case "--qual":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    qual = getValidDouble(option);
                    break;
                case "--rprs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    readPosRankSum = getValidDouble(option);
                    break;
                case "--mqrs":
                    checkValueValid(Data.NO_FILTER, Data.NO_FILTER, option);
                    mapQualRankSum = getValidDouble(option);
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

        initMinCoverage();
    }

    private static void initMinCoverage() {
        if (minCoverage != Data.NO_FILTER) {
            if (minCaseCoverageCall == Data.NO_FILTER) {
                minCaseCoverageCall = minCoverage;
            }

            if (minCaseCoverageNoCall == Data.NO_FILTER) {
                minCaseCoverageNoCall = minCoverage;
            }

            if (minCtrlCoverageCall == Data.NO_FILTER) {
                minCtrlCoverageCall = minCoverage;
            }

            if (minCtrlCoverageNoCall == Data.NO_FILTER) {
                minCtrlCoverageNoCall = minCoverage;
            }
        }

        // Broadcast coverage filters (which are the only ones not filtered using DataFrames filtering)
        int[] covCallFilters
                = {GenotypeLevelFilterCommand.minCtrlCoverageCall, GenotypeLevelFilterCommand.minCaseCoverageCall};
        int[] covNoCallFilters
                = {GenotypeLevelFilterCommand.minCtrlCoverageNoCall, GenotypeLevelFilterCommand.minCaseCoverageNoCall};

        covCallFiltersBroadcast = PopSpark.context.broadcast(covCallFilters);
        covNoCallFiltersBroadcast = PopSpark.context.broadcast(covNoCallFilters);
    }

    public static Broadcast<int[]> getCovCallFiltersBroadcast() {
        return covCallFiltersBroadcast;
    }

    public static Broadcast<int[]> getCovNoCallFiltersBroadcast() {
        return covNoCallFiltersBroadcast;
    }
}
