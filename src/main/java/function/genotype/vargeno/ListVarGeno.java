package function.genotype.vargeno;

import function.genotype.base.CalledVariant;
import function.genotype.base.Carrier;
import function.genotype.base.GenotypeLevelFilterCommand;
import function.genotype.base.NonCarrier;
import function.genotype.base.SampleManager;
import utils.SparkManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import utils.CommonCommand;

/**
 *
 * @author nick
 */
public class ListVarGeno {

    public static void run() {
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
                    HashMap<String, VarGenoOutput> varGenoOutputMap = new HashMap<>();

                    // init carrier data
                    while (calledVarRowIterator.hasNext()) {
                        Row cvRow = calledVarRowIterator.next();

                        int sampleId = cvRow.getInt(1);
                        String variantId
                        = cvRow.getString(2)        // chr
                        + "-" + cvRow.getInt(3)     // pos
                        + "-" + cvRow.getString(4)  // ref
                        + "-" + cvRow.getString(5); // alt

                        VarGenoOutput varGenoOutput = varGenoOutputMap.get(variantId);

                        if (varGenoOutput == null) {
                            CalledVariant calledVariant = new CalledVariant();
                            calledVariant.initVariantData(cvRow);
                            varGenoOutput = new VarGenoOutput(calledVariant);
                            varGenoOutputMap.put(variantId, varGenoOutput);
                        }

                        byte pheno = samplePhenoMap.get(sampleId);
                        Carrier carrier = new Carrier(cvRow, pheno);
                        varGenoOutput.getCalledVar().addCarrier(sampleId, carrier);
                        varGenoOutput.addSampleGeno(carrier.getGenotype(), pheno);
                    }

                    LinkedList<Row> outputRows = new LinkedList<>();

                    for (VarGenoOutput varGenoOutput : varGenoOutputMap.values()) {
                        // init non-carrier data
                        for (Map.Entry<Integer, Byte> samplePhenoEntry : samplePhenoMap.entrySet()) {
                            int sampleId = samplePhenoEntry.getKey();
                            byte pheno = samplePhenoEntry.getValue();

                            if (!varGenoOutput.getCalledVar().getCarrierMap().containsKey(sampleId)) { // it is non-carrier sample then
                                TreeMap<Short, Short> posCovTreeMap = sampleCovMapMap.get(sampleId);

                                if (posCovTreeMap != null) {
                                    short coverage = posCovTreeMap.floorEntry(varGenoOutput.getCalledVar().blockOffset).getValue();
                                    NonCarrier noncarrier = new NonCarrier(sampleId, coverage, pheno);
                                    varGenoOutput.getCalledVar().addNonCarrier(sampleId, noncarrier);
                                    varGenoOutput.addSampleGeno(noncarrier.getGenotype(), pheno);
                                }
                            }
                        }

                        varGenoOutput.calculate();

                        varGenoOutput.appendRowsToList(outputRows);
                    }

                    return outputRows.iterator();
                },
                RowEncoder.apply(VarGenoOutput.getSchema()));

        // Filter and output data
        outputDF = GenotypeLevelFilterCommand.applyOutputFilters(outputDF);

        // Write output
        outputDF
                .coalesce(SparkManager.session.sparkContext().defaultParallelism())
                .write()
                //.mode(i == 0 ? "overwrite" : "append")
                .mode("overwrite")
                .option("header", "true")
                .option("nullValue", "NA")
                .csv(CommonCommand.outputPath);
    }
}
