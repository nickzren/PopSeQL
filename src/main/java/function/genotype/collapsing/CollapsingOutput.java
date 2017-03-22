package function.genotype.collapsing;

import function.genotype.base.CalledVariant;
import function.genotype.base.Carrier;
import function.genotype.base.NonCarrier;
import function.genotype.base.Output;
//import function.genotype.base.Sample;
import global.Data;
import global.Index;
import java.util.LinkedList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.FormatManager;

/**
 *
 * @author nick
 */
public class CollapsingOutput extends Output {

    private double looMAF = 0;
    private String geneName;

    public CollapsingOutput(CalledVariant c) {
        super(c);
    }

    public void appendRowsToList(LinkedList<Row> l, CollapsingSummary summary) {
        for (Carrier c : calledVar.getCarrierMap().values()) {
            if (isQualifiedGeno(c.getGenotype())) {
                l.add(getRow(c));
                summary.updateSampleVariantCount4SingleVar(c.getSampleId());
            }
        }

        for (NonCarrier nc : calledVar.getNonCarrierMap().values()) {
            if (isQualifiedGeno(nc.getGenotype())) {
                l.add(getRow(nc));
                summary.updateSampleVariantCount4SingleVar(nc.getSampleId());
            }
        }
    }

    public void setGeneName(String value) {
        if (geneName == null) {
            geneName = value;
        }
    }

    public String getGeneName() {
        return geneName;
    }

    /*
     * if ref is minor then only het & ref are qualified samples. If ref is
     * major then only hom & het are qualified samples.
     */
    @Override
    public boolean isQualifiedGeno(byte geno) {
        if (CollapsingCommand.isRecessive && geno == Index.HET) {
            return false;
        }

        return super.isQualifiedGeno(geno);
    }

    // This function accepts both carrier or noncarrier instances
    public Row getRow(NonCarrier noncarrier) {

        Carrier carrier = noncarrier instanceof Carrier ? ((Carrier) noncarrier) : null;

        return RowFactory.create(
                calledVar.getVariantIdStr(),
                calledVar.getRefAllele(),
                calledVar.getAllele(),
                getGenoStr(noncarrier.getGenotype()),
                noncarrier.getSampleId(),
                noncarrier.getSamplePheno() == Index.CTRL ? "ctrl" : "case",
                genoCount[Index.HOM][Index.CASE],
                genoCount[Index.HET][Index.CASE],
                genoCount[Index.REF][Index.CASE],
                FormatManager.getFloat(minorHomFreq[Index.CASE]),
                FormatManager.getFloat(hetFreq[Index.CASE]),
                genoCount[Index.HOM][Index.CTRL],
                genoCount[Index.HET][Index.CTRL],
                genoCount[Index.REF][Index.CTRL],
                FormatManager.getFloat(minorHomFreq[Index.CTRL]),
                FormatManager.getFloat(hetFreq[Index.CTRL]),
                FormatManager.getFloat(minorAlleleFreq[Index.CASE]),
                FormatManager.getFloat(minorAlleleFreq[Index.CTRL]),
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
            DataTypes.createStructField("Ref Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Alt Allele", DataTypes.StringType, true),
            DataTypes.createStructField("Genotype", DataTypes.StringType, true),
            DataTypes.createStructField("Sample Name", DataTypes.IntegerType, true),
            DataTypes.createStructField("Sample Type", DataTypes.StringType, true),
            DataTypes.createStructField("Hom Case", DataTypes.IntegerType, true),
            DataTypes.createStructField("Het Case", DataTypes.IntegerType, true),
            DataTypes.createStructField("Hom Ref Case", DataTypes.IntegerType, true),
            DataTypes.createStructField("Minor Hom Case Freq", DataTypes.StringType, true),
            DataTypes.createStructField("Het Case Freq", DataTypes.StringType, true),
            DataTypes.createStructField("Hom Ctrl", DataTypes.IntegerType, true),
            DataTypes.createStructField("Het Ctrl", DataTypes.IntegerType, true),
            DataTypes.createStructField("Hom Ref Ctrl", DataTypes.IntegerType, true),
            DataTypes.createStructField("Minor Hom Ctrl Freq", DataTypes.StringType, true),
            DataTypes.createStructField("Het Ctrl Freq", DataTypes.StringType, true),
            DataTypes.createStructField("Case Maf", DataTypes.StringType, true),
            DataTypes.createStructField("Ctrl Maf", DataTypes.StringType, true),
            DataTypes.createStructField("Samtools Raw Coverage", DataTypes.StringType, true),
            DataTypes.createStructField("Gatk Filtered Coverage", DataTypes.StringType, true),
            DataTypes.createStructField("Reads Alt", DataTypes.StringType, true),
            DataTypes.createStructField("Reads Ref", DataTypes.StringType, true),
            DataTypes.createStructField("Percent Alt Read", DataTypes.StringType, true),
            DataTypes.createStructField("Vqslod", DataTypes.StringType, true),
            DataTypes.createStructField("Pass Fail Status", DataTypes.StringType, true),
            DataTypes.createStructField("Genotype Qual GQ", DataTypes.StringType, true),
            DataTypes.createStructField("Strand Bias FS", DataTypes.StringType, true),
            DataTypes.createStructField("Haplotype Score", DataTypes.StringType, true),
            DataTypes.createStructField("Rms Map Qual MQ", DataTypes.StringType, true),
            DataTypes.createStructField("Qual By Depth QD", DataTypes.StringType, true),
            DataTypes.createStructField("Qual", DataTypes.StringType, true),
            DataTypes.createStructField("Read Pos Rank Sum", DataTypes.StringType, true),
            DataTypes.createStructField("Map Qual Rank Sum", DataTypes.StringType, true),};
        return new StructType(fields);
    }
}
