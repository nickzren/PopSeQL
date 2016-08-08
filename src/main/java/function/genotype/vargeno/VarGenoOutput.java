package function.genotype.vargeno;

import function.genotype.base.CalledVariant;
import function.variant.base.Output;
import global.Index;
import function.genotype.base.Carrier;
import function.genotype.base.GenotypeLevelFilterCommand;
import function.genotype.base.NonCarrier;
import function.genotype.base.SampleManager;
import global.Data;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import utils.FormatManager;
import utils.MathManager;

/**
 *
 * @author nick
 */
public class VarGenoOutput extends Output {

//    private double percentAltReadBinomialP = Data.NA;
    
    public static String[] colsToBeDropped = { "var_present", "case_carrier" };
    
    public List<String> getRowStrings() {
        LinkedList<String> l = new LinkedList<>();
        for (Carrier c : calledVar.getCarriers()) {
            l.add(getString(c));
        }
        return l;
    }
    
    public List<Row> getRows() {
        LinkedList<Row> l = new LinkedList<>();
        for (Carrier c : calledVar.getCarriers()) {
            l.add(getRow(c));
        }
        return l;
    }

    public static String getTitle() {
        return "Variant ID,"
                + "Variant Type,"
//                + "Rs Number,"
                + "Ref Allele,"
                + "Alt Allele,"
//                + "CADD Score Phred,"
//                + GerpManager.getTitle()
//                + TrapManager.getTitle()
                + "Is Minor Ref,"
                + "Genotype,"
                + "Sample Name,"
                + "Sample Type,"
                + "Major Hom Case,"
                + "Het Case,"
                + "Minor Hom Case,"
                + "Minor Hom Case Freq,"
                + "Het Case Freq,"
                + "Major Hom Ctrl,"
                + "Het Ctrl,"
                + "Minor Hom Ctrl,"
                + "Minor Hom Ctrl Freq,"
                + "Het Ctrl Freq,"
                + "Missing Case,"
                + "QC Fail Case,"
                + "Missing Ctrl,"
                + "QC Fail Ctrl,"
                + "Case Maf,"
                + "Ctrl Maf,"
                + "Case HWE_P,"
                + "Ctrl HWE_P,"
                + "Samtools Raw Coverage,"
                + "Gatk Filtered Coverage,"
                + "Reads Alt,"
                + "Reads Ref,"
                + "Percent Alt Read,"
//                + "Percent Alt Read Binomial P,"
                + "Vqslod,"
                + "Pass Fail Status,"
                + "Genotype Qual GQ,"
                + "Strand Bias FS,"
                + "Haplotype Score,"
                + "Rms Map Qual MQ,"
                + "Qual By Depth QD,"
                + "Qual,"
                + "Read Pos Rank Sum,"
                + "Map Qual Rank Sum,"
//                + EvsManager.getTitle()
//                + "Polyphen Humdiv Score,"
//                + "Polyphen Humdiv Prediction,"
//                + "Polyphen Humvar Score,"
//                + "Polyphen Humvar Prediction,"
//                + "Function,"
//                + "Gene Name,"
//                + "Artifacts in Gene,"
//                + "Codon Change,"
//                + "Gene Transcript (AA Change),"
//                + ExacManager.getTitle()
//                + KaviarManager.getTitle()
//                + KnownVarManager.getTitle()
//                + RvisManager.getTitle()
//                + SubRvisManager.getTitle()
//                + GenomesManager.getTitle()
//                + MgiManager.getTitle()
                ;
    }

    public VarGenoOutput(CalledVariant c) {
        super(c);
    }

    public String getString(Carrier carrier) {
        StringBuilder sb = new StringBuilder();

//        Carrier carrier = calledVar.getCarrier(sample.getId());
        int readsAlt = carrier != null ? carrier.getReadsAlt() : Data.NA;
        int readsRef = carrier != null ? carrier.getReadsRef() : Data.NA;
        
        sb.append(calledVar.getVariantIdStr()).append(",");
        sb.append(calledVar.getType()).append(",");
//        sb.append(calledVar.getRsNumber()).append(",");
        sb.append(calledVar.getRefAllele()).append(",");
        sb.append(calledVar.getAllele()).append(",");
//        sb.append(FormatManager.getDouble(calledVar.getCscore())).append(",");
//        sb.append(calledVar.getGerpScore());
//        sb.append(calledVar.getTrapScore());
        sb.append(isMinorRef).append(",");
        sb.append(getGenoStr(carrier.getGenotype())).append(","); //sb.append(getGenoStr(calledVar.getGenotype(sample.getIndex()))).append(",");
        sb.append(carrier.getSampleId()).append(",");
        sb.append(carrier.getSamplePheno()).append(",");
        sb.append(majorHomCount[Index.CASE]).append(",");
        sb.append(genoCount[Index.HET][Index.CASE]).append(",");
        sb.append(minorHomCount[Index.CASE]).append(",");
        sb.append(FormatManager.getDouble(minorHomFreq[Index.CASE])).append(",");
        sb.append(FormatManager.getDouble(hetFreq[Index.CASE])).append(",");
        sb.append(majorHomCount[Index.CTRL]).append(",");
        sb.append(genoCount[Index.HET][Index.CTRL]).append(",");
        sb.append(minorHomCount[Index.CTRL]).append(",");
        sb.append(FormatManager.getDouble(minorHomFreq[Index.CTRL])).append(",");
        sb.append(FormatManager.getDouble(hetFreq[Index.CTRL])).append(",");
        sb.append(genoCount[Index.MISSING][Index.CASE]).append(",");
        sb.append(calledVar.getQcFailSample(Index.CASE)).append(",");
        sb.append(genoCount[Index.MISSING][Index.CTRL]).append(",");
        sb.append(calledVar.getQcFailSample(Index.CTRL)).append(",");
        sb.append(FormatManager.getDouble(minorAlleleFreq[Index.CASE])).append(",");
        sb.append(FormatManager.getDouble(minorAlleleFreq[Index.CTRL])).append(",");
        sb.append(FormatManager.getDouble(hweP[Index.CASE])).append(",");
        sb.append(FormatManager.getDouble(hweP[Index.CTRL])).append(",");
        sb.append(FormatManager.getDouble(carrier.getCoverage())).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getGatkFilteredCoverage() : Data.NA)).append(",");
        sb.append(FormatManager.getInteger(readsAlt)).append(",");
        sb.append(FormatManager.getInteger(readsRef)).append(",");
        sb.append(FormatManager.getPercAltRead(readsAlt, carrier != null ? carrier.getGatkFilteredCoverage() : Data.NA)).append(",");
//        sb.append(FormatManager.getDouble(MathManager.getBinomial(readsAlt + readsRef, readsAlt, 0.5))).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getVqslod() : Data.NA)).append(",");
        sb.append(carrier != null ? carrier.getPassFailStatus() : "NA").append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getGenotypeQualGQ() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getStrandBiasFS() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getHaplotypeScore() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getRmsMapQualMQ() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getQualByDepthQD() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getQual() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getReadPosRankSum() : Data.NA)).append(",");
        sb.append(FormatManager.getDouble(carrier != null ? carrier.getMapQualRankSum() : Data.NA)).append(",");
//        sb.append(calledVar.getEvsStr());
//        sb.append(calledVar.getPolyphenHumdivScore()).append(",");
//        sb.append(calledVar.getPolyphenHumdivPrediction()).append(",");
//        sb.append(calledVar.getPolyphenHumvarScore()).append(",");
//        sb.append(calledVar.getPolyphenHumvarPrediction()).append(",");
//        sb.append(calledVar.getFunction()).append(",");
//        sb.append("'").append(calledVar.getGeneName()).append("'").append(",");
//        sb.append(FormatManager.getInteger(GeneManager.getGeneArtifacts(calledVar.getGeneName()))).append(",");
//        sb.append(calledVar.getCodonChange()).append(",");
//        sb.append(calledVar.getTranscriptSet()).append(",");
//        sb.append(calledVar.getExacStr());
//        sb.append(calledVar.getKaviarStr());
//        sb.append(calledVar.getKnownVarStr());
//        sb.append(calledVar.getRvis());
//        sb.append(calledVar.getSubRvis());
//        sb.append(calledVar.get1000Genomes());
//        sb.append(calledVar.getMgi());

        return sb.toString();
    }
    
    // This function accepts both carrier or noncarrier instances
    public Row getRow(NonCarrier noncarrier) {
        
        Carrier carrier = noncarrier instanceof Carrier ? ((Carrier) noncarrier) : null;

        int readsAlt = carrier != null ? carrier.getReadsAlt() : Data.NA;
        int readsRef = carrier != null ? carrier.getReadsRef() : Data.NA;
        
        return RowFactory.create(
            calledVar.getVariantIdStr(),
            calledVar.getType(),
    //        calledVar.getRsNumber(),
            calledVar.getRefAllele(),
            calledVar.getAllele(),
    //        FormatManager.getDouble(calledVar.getCscore()),
    //        calledVar.getGerpScore());
    //        calledVar.getTrapScore());
            isMinorRef ? 1 : 0,
            getGenoStr(noncarrier.getGenotype()), //getGenoStr(calledVar.getGenotype(sample.getIndex())),
            noncarrier.getSampleId(), // Attention: Right now it is IntegerType on the schema
            noncarrier.getSamplePheno() == 0 ? "case" : "ctrl" ,
            majorHomCount[Index.CASE],
            genoCount[Index.HET][Index.CASE],
            minorHomCount[Index.CASE],
            FormatManager.getDouble(minorHomFreq[Index.CASE]),
            FormatManager.getDouble(hetFreq[Index.CASE]),
            majorHomCount[Index.CTRL],
            genoCount[Index.HET][Index.CTRL],
            minorHomCount[Index.CTRL],
            FormatManager.getDouble(minorHomFreq[Index.CTRL]),
            FormatManager.getDouble(hetFreq[Index.CTRL]),
            genoCount[Index.MISSING][Index.CASE],
            calledVar.getQcFailSample(Index.CASE),
            genoCount[Index.MISSING][Index.CTRL],
            calledVar.getQcFailSample(Index.CTRL),
            FormatManager.getDoubleObject(minorAlleleFreq[Index.CASE]), // keep double
            FormatManager.getDoubleObject(minorAlleleFreq[Index.CTRL]), //keep double
            FormatManager.getDouble(hweP[Index.CASE]),
            FormatManager.getDouble(hweP[Index.CTRL]),
            FormatManager.getDouble(noncarrier.getCoverage()),
            FormatManager.getDouble(carrier != null ? carrier.getGatkFilteredCoverage() : Data.NA),
            FormatManager.getIntegerObject(readsAlt),
            FormatManager.getIntegerObject(readsRef),
            FormatManager.getPercAltRead(readsAlt, carrier != null ? carrier.getGatkFilteredCoverage() : Data.NA),
    //        FormatManager.getDouble(MathManager.getBinomial(readsAlt + readsRef, readsAlt, 0.5)),
            FormatManager.getDouble(carrier != null ? carrier.getVqslod() : Data.NA),
            carrier != null ? carrier.getPassFailStatus() : "NA",
            FormatManager.getDouble(carrier != null ? carrier.getGenotypeQualGQ() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getStrandBiasFS() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getHaplotypeScore() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getRmsMapQualMQ() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getQualByDepthQD() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getQual() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getReadPosRankSum() : Data.NA),
            FormatManager.getDouble(carrier != null ? carrier.getMapQualRankSum() : Data.NA),
            getVarPresent(),
            (GenotypeLevelFilterCommand.minCaseCarrier != Data.NO_FILTER ? getCaseCarrier() : null)
    //        calledVar.getEvsStr());
    //        calledVar.getPolyphenHumdivScore(),
    //        calledVar.getPolyphenHumdivPrediction(),
    //        calledVar.getPolyphenHumvarScore(),
    //        calledVar.getPolyphenHumvarPrediction(),
    //        calledVar.getFunction(),
    //        "'").append(calledVar.getGeneName()).append("'",
    //        FormatManager.getInteger(GeneManager.getGeneArtifacts(calledVar.getGeneName())),
    //        calledVar.getCodonChange(),
    //        calledVar.getTranscriptSet(),
    //        calledVar.getExacStr());
    //        calledVar.getKaviarStr());
    //        calledVar.getKnownVarStr());
    //        calledVar.getRvis());
    //        calledVar.getSubRvis());
    //        calledVar.get1000Genomes());
    //        calledVar.getMgi());
        );
    }
    
    public static StructType getSchema() {
        StructField[] fields = {
            DataTypes.createStructField("Variant ID", DataTypes.StringType, true),
            DataTypes.createStructField("Variant Type", DataTypes.StringType, true),
            DataTypes.createStructField("Ref Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Alt Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Is Minor Ref", DataTypes.IntegerType, true),
            DataTypes.createStructField("Genotype", DataTypes.StringType, true),
            DataTypes.createStructField("Sample Name", DataTypes.IntegerType, true),
            DataTypes.createStructField("Sample Type", DataTypes.StringType, true),
            DataTypes.createStructField("Major Hom Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Het Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Case Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Het Case Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Major Hom Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Het Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Minor Hom Ctrl Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Het Ctrl Freq", DataTypes.StringType, true),//d
            DataTypes.createStructField("Missing Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("QC Fail Case", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Missing Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("QC Fail Ctrl", DataTypes.IntegerType, true),//i
            DataTypes.createStructField("Case Maf", DataTypes.DoubleType, true),//d
            DataTypes.createStructField("Ctrl Maf", DataTypes.DoubleType, true),//d
            DataTypes.createStructField("Case HWE_P", DataTypes.StringType, true),//d
            DataTypes.createStructField("Ctrl HWE_P", DataTypes.StringType, true),//d
            DataTypes.createStructField("Samtools Raw Coverage", DataTypes.StringType, true),//s
            DataTypes.createStructField("Gatk Filtered Coverage", DataTypes.StringType, true),//s
            DataTypes.createStructField("Reads Alt", DataTypes.IntegerType, true),//s
            DataTypes.createStructField("Reads Ref", DataTypes.IntegerType, true),//s
            DataTypes.createStructField("Percent Alt Read", DataTypes.StringType, true),//d
            DataTypes.createStructField("Vqslod", DataTypes.StringType, true),//d
            DataTypes.createStructField("Pass Fail Status", DataTypes.StringType, true),
            DataTypes.createStructField("Genotype Qual GQ", DataTypes.StringType, true),//d
            DataTypes.createStructField("Strand Bias FS", DataTypes.StringType, true),//d
            DataTypes.createStructField("Haplotype Score", DataTypes.StringType, true),//d
            DataTypes.createStructField("Rms Map Qual MQ", DataTypes.StringType, true),//d
            DataTypes.createStructField("Qual By Depth QD", DataTypes.StringType, true),//d
            DataTypes.createStructField("Qual", DataTypes.StringType, true),//d
            DataTypes.createStructField("Read Pos Rank Sum", DataTypes.StringType, true),//d
            DataTypes.createStructField("Map Qual Rank Sum", DataTypes.StringType, true),//d
            
            //extra fields - update colsToBeDropped with changes !!
            DataTypes.createStructField("var_present", DataTypes.IntegerType, true),
            DataTypes.createStructField("case_carrier", DataTypes.IntegerType, true),
            
        };
        return new StructType(fields);
    }
    
}
