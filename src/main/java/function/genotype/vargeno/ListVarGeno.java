package function.genotype.vargeno;

import function.genotype.base.CalledVariant;
import function.genotype.base.CalledVariantSparkUtils;
import function.genotype.base.CarrierSparkUtils;
import function.genotype.base.NonCarrier;
import function.genotype.base.SampleManager;
import utils.PopSpark;
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
 * @author felipe, nick
 */
public class ListVarGeno {

    public static void run() {
        // init called_variant data
        System.out.println(">>> init called_variant dat");

        Dataset<Row> calledVarDF = CalledVariantSparkUtils.getCalledVariantDF();

        Dataset<Row> filteredCalledVarDF = CarrierSparkUtils.applyCarrierFilters(calledVarDF);

        KeyValueGroupedDataset<String, Row> groupedCalledVarDF = filteredCalledVarDF.groupByKey(
                (Row r) -> r.getString(0), // group by block id
                Encoders.STRING());

        // init read_coverage data
        System.out.println("\t> init coverage data");
        Dataset<Row> covDF
                = CalledVariantSparkUtils.getReadCoverageDF();

        KeyValueGroupedDataset<String, Row> groupedCoverageDF = covDF.groupByKey(
                (Row r) -> r.getString(0), // group by block id
                Encoders.STRING());

        // init output dataframe
        Dataset<Row> outputDF = groupedCalledVarDF.cogroup(groupedCoverageDF,
                (String k, Iterator<Row> calledVarIterator, Iterator<Row> covIterator) -> {
                    CalledVariantSparkUtils.initCovFilters();
                    /* Parse coverage blocks */
                    HashMap<Integer, TreeMap<Short, Short>> sampleCovMapMap = new HashMap<>();
                    // \-> Maps each sample_id to a tree map that maps the block offset to the coverage value
                    while (covIterator.hasNext()) {
                        Row covBlockRow = covIterator.next();
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

                    HashMap<Integer, HashMap<Integer, Short>> variantSamplePhenoMapMap = new HashMap<>();
                    // \-> Maps variant_id to a sampleMap copy
                    //      (a map that maps sample_id to pheno)
                    // There will be one copy for each variant,
                    //      that is used to keep track of the non-carriers to be created
                    // Such copies are created by cloning the broadcasted value

                    HashMap<Integer, CalledVariant> calledVariantMap = new HashMap<>();
                    // \-> Maps the variant_id to the CalledVariant object

                    // Iterate thru called_variant rows and add its  carrier data to the respective CalledVariant object
                    // (create one object, if it is being seen for the first time)
                    while (calledVarIterator.hasNext()) {
                        Row cvRow = calledVarIterator.next();

                        int sampleId = cvRow.getInt(1);
                        int variantId = cvRow.getInt(2);

                        HashMap<Integer, Short> samplePhenoMap;
                        CalledVariant calledVariant= calledVariantMap.get(variantId);
                        if (calledVariant != null) {
                            samplePhenoMap = variantSamplePhenoMapMap.get(variantId);
                        } else {
                            samplePhenoMap = (HashMap<Integer, Short>) SampleManager.getSampleMapBroadcast().value().clone();
                            variantSamplePhenoMapMap.put(variantId, samplePhenoMap);
                            calledVariant = new CalledVariant();
                            calledVariant.initVariantData(cvRow);
                            calledVariantMap.put(variantId, calledVariant);
                        }

                        short pheno = samplePhenoMap.get(sampleId);
                        calledVariant.addCarrier(cvRow, sampleId, pheno);
                        samplePhenoMap.remove(sampleId);
                    }

                    // For each variant that was read, get the sample map copy, which,
                    //      at this point, only contains the samples that are non-carriers
                    LinkedList<Row> outputRows = new LinkedList<>();
                    // \-> List that holds output rows
                    for (Map.Entry<Integer, HashMap<Integer, Short>> samplePhenoMapEntry : variantSamplePhenoMapMap.entrySet()) {
                        int variantId = samplePhenoMapEntry.getKey();
                        HashMap<Integer, Short> samplePhenoMap = samplePhenoMapEntry.getValue();

                        // Get CalledVariant object
                        CalledVariant calledVariant = calledVariantMap.get(variantId);

                        // For each non sample that is a non-carrier, retrieve the coverage
                        //      and create a NonCarrier object
                        for (Map.Entry<Integer, Short> samplePhenoEntry : samplePhenoMap.entrySet()) {
                            int sampleId = samplePhenoEntry.getKey();
                            short pheno = samplePhenoEntry.getValue();

                            short coverage;
                            if (sampleCovMapMap.containsKey(sampleId)) {
                                coverage = sampleCovMapMap.get(sampleId).floorEntry(calledVariant.blockOffset).getValue();
                                calledVariant.addNonCarrier(sampleId, coverage, pheno);
                            }
//                            else
//                                coverage = Data.NA;

//                            calledVariant.addNonCarrier(sampleId, coverage, pheno);
                        }

                        // Build output
                        VarGenoOutput out = new VarGenoOutput(calledVariant);
                        calledVariant.addSampleDataToOutput(out);
                        out.calculate();

                        // Append output rows to the output list
                        out.appendRowsToList(outputRows);
                    }

                    return outputRows.iterator();
                },
                RowEncoder.apply(VarGenoOutput.getSchema()));

        ///////////////////
        // Filter and output data
        ///////////////////
        // Filter rows based on output filters
        outputDF = CalledVariantSparkUtils.applyOutputFilters(outputDF);

        // Drop columns that are used for output filters only
        outputDF = outputDF.drop(VarGenoOutput.colsToBeDropped);

        // Write output
        outputDF
                .coalesce(PopSpark.session.sparkContext().defaultParallelism())
                .write()
                //.mode(i == 0 ? "overwrite" : "append")
                .mode("overwrite")
                .option("header", "true")
                .option("nullValue", "NA")
                .csv(CommonCommand.realOutputPath);
    }
}
