package function.genotype.vargeno;

import function.genotype.base.CalledVariant;
import function.variant.base.Output;
import global.Index;
import function.genotype.base.Carrier;
import function.genotype.base.NonCarrier;
import global.Data;
import java.util.LinkedList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import utils.FormatManager;

/**
 *
 * @author nick
 */
public class VarGenoOutput extends Output {

    public void appendRowsToList(LinkedList<Row> l) {
        for (Carrier c : calledVar.getCarrierMap().values()) {
            if (isQualifiedGeno(c.getGenotype())) {
                l.add(getRow(c));
            }
        }

        for (NonCarrier nc : calledVar.getNonCarrierMap().values()) {
            if (isQualifiedGeno(nc.getGenotype())) {
                l.add(getRow(nc));
            }
        }
    }

    public static String getTitle() {
        return "Variant ID,"
                + "Variant Type,"
                + "Ref Allele,"
                + "Alt Allele,"
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
                + "QC Fail Case,"
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
                + "Vqslod,"
                + "Pass Fail Status,"
                + "Genotype Qual GQ,"
                + "Strand Bias FS,"
                + "Haplotype Score,"
                + "Rms Map Qual MQ,"
                + "Qual By Depth QD,"
                + "Qual,"
                + "Read Pos Rank Sum,"
                + "Map Qual Rank Sum,";
    }

    public VarGenoOutput(CalledVariant c) {
        super(c);
    }

    // This function accepts both carrier or noncarrier instances
    public Row getRow(NonCarrier noncarrier) {

        Carrier carrier = noncarrier instanceof Carrier ? ((Carrier) noncarrier) : null;

        return RowFactory.create(
                calledVar.getVariantIdStr(),
                calledVar.getType(),
                calledVar.getRefAllele(),
                calledVar.getAllele(),
                isMinorRef,
                getGenoStr(noncarrier.getGenotype()),
                noncarrier.getSampleId(),
                noncarrier.getSamplePheno() == Index.CTRL ? "ctrl" : "case",
                majorHomCount[Index.CASE],
                genoCount[Index.HET][Index.CASE],
                minorHomCount[Index.CASE],
                FormatManager.getFloat(minorHomFreq[Index.CASE]),
                FormatManager.getFloat(hetFreq[Index.CASE]),
                majorHomCount[Index.CTRL],
                genoCount[Index.HET][Index.CTRL],
                minorHomCount[Index.CTRL],
                FormatManager.getFloat(minorHomFreq[Index.CTRL]),
                FormatManager.getFloat(hetFreq[Index.CTRL]),
                FormatManager.getFloat(minorAlleleFreq[Index.CASE]),
                FormatManager.getFloat(minorAlleleFreq[Index.CTRL]),
                FormatManager.getDouble(hweP[Index.CASE]),
                FormatManager.getDouble(hweP[Index.CTRL]),
                FormatManager.getInteger(noncarrier.getCoverage()),
                FormatManager.getInteger(carrier != null ? carrier.getGatkFilteredCoverage() : Data.INTEGER_NA),
                FormatManager.getShort(carrier != null ? carrier.getReadsAlt() : Data.SHORT_NA),
                FormatManager.getShort(carrier != null ? carrier.getReadsAlt() : Data.SHORT_NA),
                carrier != null ? carrier.getPercAltRead() : Data.STRING_NA,
                FormatManager.getFloat(carrier != null ? carrier.getVqslod() : Data.FLOAT_NA),
                carrier != null ? carrier.getPassFailStatus() : Data.STRING_NA,
                FormatManager.getFloat(carrier != null ? carrier.getGenotypeQualGQ() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getStrandBiasFS() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getHaplotypeScore() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getRmsMapQualMQ() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getQualByDepthQD() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getQual() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getReadPosRankSum() : Data.FLOAT_NA),
                FormatManager.getFloat(carrier != null ? carrier.getMapQualRankSum() : Data.FLOAT_NA)
        );
    }

    public static StructType getSchema() {
        StructField[] fields = {
            DataTypes.createStructField("Variant ID", DataTypes.StringType, true),
            DataTypes.createStructField("Variant Type", DataTypes.StringType, true),
            DataTypes.createStructField("Ref Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Alt Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Is Minor Ref", DataTypes.BooleanType, true),
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
            DataTypes.createStructField("Case Maf", DataTypes.StringType, true),//d
            DataTypes.createStructField("Ctrl Maf", DataTypes.StringType, true),//d
            DataTypes.createStructField("Case HWE_P", DataTypes.StringType, true),//d
            DataTypes.createStructField("Ctrl HWE_P", DataTypes.StringType, true),//d
            DataTypes.createStructField("Samtools Raw Coverage", DataTypes.StringType, true),//s
            DataTypes.createStructField("Gatk Filtered Coverage", DataTypes.StringType, true),//s
            DataTypes.createStructField("Reads Alt", DataTypes.StringType, true),//s
            DataTypes.createStructField("Reads Ref", DataTypes.StringType, true),//s
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
        };
        return new StructType(fields);
    }

}
