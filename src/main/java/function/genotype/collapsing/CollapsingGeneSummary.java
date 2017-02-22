package function.genotype.collapsing;

import utils.FormatManager;

/**
 *
 * @author nick
 */
public class CollapsingGeneSummary extends CollapsingSummary {

    // output columns 
    public static String getTitle() {
        return "Rank,"
                + "Gene Name,"
                + "Total Variant,"
                + "Total SNV,"
                + "Total Indel,"
                + "Qualified Case,"
                + "Unqualified Case,"
                + "Qualified Case Freq,"
                + "Qualified Ctrl,"
                + "Unqualified Ctrl,"
                + "Qualified Ctrl Freq,"
                + "Enriched Direction,"
                + "Fet P,"
                + "Linear P,"
                + "Logistic P,";
    }

    String coverageSummaryLine;

    public CollapsingGeneSummary(String name) {
        super(name);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("'").append(name).append("'").append(",");
        sb.append(totalVariant.value()).append(",");
        sb.append(totalSnv.value()).append(",");
        sb.append(totalIndel.value()).append(",");
        sb.append(qualifiedCase).append(",");
        sb.append(unqualifiedCase).append(",");
        sb.append(FormatManager.getDouble(qualifiedCaseFreq)).append(",");
        sb.append(qualifiedCtrl).append(",");
        sb.append(unqualifiedCtrl).append(",");
        sb.append(FormatManager.getDouble(qualifiedCtrlFreq)).append(",");
        sb.append(enrichedDirection).append(",");
        sb.append(FormatManager.getDouble(fetP)).append(",");
        sb.append(FormatManager.getDouble(linearP)).append(",");
        sb.append(FormatManager.getDouble(logisticP)).append(",");

        return sb.toString();
    }
}
