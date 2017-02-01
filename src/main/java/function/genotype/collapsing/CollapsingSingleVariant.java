package function.genotype.collapsing;

import function.genotype.base.CalledVariant;
import function.genotype.base.Carrier;
import function.genotype.base.Gene;
import function.genotype.base.GeneManager;
import function.genotype.base.GenotypeLevelFilterCommand;
import function.genotype.base.NonCarrier;
import function.genotype.base.SampleManager;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import utils.CommonCommand;
import utils.SparkManager;

/**
 *
 * @author nick
 */
public class CollapsingSingleVariant {

    public static void run() {
        HashMap<String, TreeMap<Integer, Gene>> geneMap = GeneManager.getGeneMapBroadcast().value();

        ArrayList<CollapsingGeneSummary> summaryList = new ArrayList<>();
        HashMap<String, CollapsingGeneSummary> summaryMap = new HashMap<>();

        // var geno data
        HashMap<Integer, Byte> samplePhenoMap = SampleManager.getSampleMapBroadcast().value();

        // init called_variant data
        Dataset<Row> calledVarDF = GenotypeLevelFilterCommand.getCalledVariantDF();

        Dataset<Row> filteredCalledVarDF = GenotypeLevelFilterCommand.applyCarrierFilters(calledVarDF);

        KeyValueGroupedDataset<String, Row> groupedCalledVarDF = filteredCalledVarDF.groupByKey(
                (Row r) -> r.getString(0), // group by block id
                Encoders.STRING());

        // init read_coverage data
        Dataset<Row> covDF
                = GenotypeLevelFilterCommand.getReadCoverageDF();

        KeyValueGroupedDataset<String, Row> groupedCoverageDF = covDF.groupByKey(
                (Row r) -> r.getString(0), // group by block id
                Encoders.STRING());

        // init output data
        Dataset<Row> outputDF = groupedCalledVarDF.cogroup(groupedCoverageDF,
                (String k, Iterator<Row> calledVarRowIterator, Iterator<Row> covRowIterator) -> {
                    // init covRowIterator data
                    HashMap<Integer, TreeMap<Short, Short>> sampleCovMapMap = new HashMap<>();
                    // \-> Maps each sample_id to a tree map that maps the block offset to the coverage value
                    while (covRowIterator.hasNext()) {
                        Row covBlockRow = covRowIterator.next();
                        int sampleId = covBlockRow.getInt(1);
                        String[] covPieces = covBlockRow.getString(2).split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");

                        TreeMap<Short, Short> tm = new TreeMap<>();

                        short pos = 0;
                        for (int index = 0; index < covPieces.length; index += 2) {
                            tm.put(pos, NonCarrier.getCovValue(covPieces[index + 1].charAt(0)));
                            pos += Short.parseShort(covPieces[index]);
                        }

                        sampleCovMapMap.put(sampleId, tm);
                    }

                    // init calledVarRowIterator data
                    HashMap<String, CollapsingOutput> varGenoOutputMap = new HashMap<>();

                    // init carrier data
                    while (calledVarRowIterator.hasNext()) {
                        Row cvRow = calledVarRowIterator.next();

                        int sampleId = cvRow.getInt(1);
                        String variantId
                        = cvRow.getString(2) // chr
                        + "-" + cvRow.getInt(3) // pos
                        + "-" + cvRow.getString(4) // ref
                        + "-" + cvRow.getString(5); // alt

                        CollapsingOutput varGenoOutput = varGenoOutputMap.get(variantId);

                        if (varGenoOutput == null) {
                            CalledVariant calledVariant = new CalledVariant();
                            calledVariant.initVariantData(cvRow);
                            varGenoOutput = new CollapsingOutput(calledVariant);
                            varGenoOutputMap.put(variantId, varGenoOutput);
                        }

                        byte pheno = samplePhenoMap.get(sampleId);
                        Carrier carrier = new Carrier(cvRow, pheno);
                        varGenoOutput.getCalledVar().addCarrier(sampleId, carrier);
                        varGenoOutput.addSampleGeno(carrier.getGenotype(), pheno);
                    }

                    LinkedList<Row> outputRows = new LinkedList<>();

                    for (CollapsingOutput output : varGenoOutputMap.values()) {
                        // init non-carrier data
                        for (Map.Entry<Integer, Byte> samplePhenoEntry : samplePhenoMap.entrySet()) {
                            int sampleId = samplePhenoEntry.getKey();
                            byte pheno = samplePhenoEntry.getValue();

                            if (!output.getCalledVar().getCarrierMap().containsKey(sampleId)) { // it is non-carrier sample then
                                TreeMap<Short, Short> posCovTreeMap = sampleCovMapMap.get(sampleId);

                                if (posCovTreeMap != null) {
                                    short coverage = posCovTreeMap.floorEntry(output.getCalledVar().blockOffset).getValue();
                                    NonCarrier noncarrier = new NonCarrier(sampleId, coverage, pheno);
                                    output.getCalledVar().addNonCarrier(sampleId, noncarrier);
                                    output.addSampleGeno(noncarrier.getGenotype(), pheno);
                                }
                            }
                        }

                        output.calculate();

                        // filter variants
                        if (output.isValid()) {
                            Entry<Integer, Gene> entry
                            = geneMap
                            .get(output.getCalledVar().chrStr)
                            .floorEntry(output.getCalledVar().position);

                            if (entry != null) {
                                Gene gene = entry.getValue();

                                if (gene.contains(output.getCalledVar().position)) {
                                    CollapsingGeneSummary summary = summaryMap.get(gene.getName());

                                    if (summary == null) {
                                        summary = new CollapsingGeneSummary(gene.getName());

                                        summaryMap.put(gene.getName(), summary);
                                    }

                                    summary.updateVariantCount(output);

                                    // prepare genotypes.csv output data
                                    output.appendRowsToList(outputRows, summary);
                                }
                            }
                        }
                    }

                    return outputRows.iterator();
                },
                RowEncoder.apply(CollapsingOutput.getSchema()));

        // Write output
        outputDF
                .coalesce(SparkManager.session.sparkContext().defaultParallelism())
                .write()
                //.mode(i == 0 ? "overwrite" : "append")
                .mode("overwrite")
                .option("header", "true")
                .option("nullValue", "NA")
                .csv(CommonCommand.outputPath);

        
        // collapsing summary & matrix
        summaryList.addAll(summaryMap.values());

        outputMatrix(summaryList, samplePhenoMap);

        Collections.sort(summaryList);

        System.out.println("Gene size: " + summaryMap.size());

        try {
            BufferedWriter bwSummary = new BufferedWriter(new FileWriter(
                    CommonCommand.outputPath + File.separator + "summary.csv"));

            bwSummary.write(CollapsingGeneSummary.getTitle());

            int rank = 1;
            for (CollapsingGeneSummary summary : summaryList) {
                bwSummary.write(rank++ + ",");
                bwSummary.write(summary.toString());
                bwSummary.newLine();
            }

            bwSummary.flush();
            bwSummary.close();
        } catch (IOException ex) {
            Logger.getLogger(CollapsingSingleVariant.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void outputMatrix(ArrayList<CollapsingGeneSummary> summaryList,
            HashMap<Integer, Byte> samplePhenoMap) {
        try {
            BufferedWriter bwSampleMatrix = new BufferedWriter(new FileWriter(
                    CommonCommand.outputPath + File.separator + "matrix.txt"));

            bwSampleMatrix.write("sample/gene" + "\t");

            for (Map.Entry<Integer, Byte> entry : samplePhenoMap.entrySet()) {
                bwSampleMatrix.write(entry.getKey() + "\t");
            }

            for (CollapsingSummary summary : summaryList) {
                bwSampleMatrix.write(summary.name + "\t");

                for (Map.Entry<Integer, Byte> entry : samplePhenoMap.entrySet()) {
                    bwSampleMatrix.write(summary.sampleVariantCountMap.get(entry.getKey()) + "\t");
                }
                bwSampleMatrix.newLine();

                summary.countSample(samplePhenoMap);

                summary.calculateFetP();
            }

            bwSampleMatrix.flush();
            bwSampleMatrix.close();
        } catch (Exception ex) {
            Logger.getLogger(CollapsingSingleVariant.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
