package function.genotype.vargeno;

import function.genotype.base.CalledVariant;
import function.genotype.base.CalledVariantSparkUtils;
import function.genotype.base.CarrierSparkUtils;
import function.genotype.base.GenotypeLevelFilterCommand;
import function.genotype.base.NonCarrierSparkUtils;
import function.genotype.base.SampleManager;
import global.PopSpark;
import global.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import utils.CommonCommand;

/**
 *
 * @author felipe
 */
public class ListVarGeno {
    
    public static final int STEPSIZE = 1000;
    
    public static void run() {
        System.out.println(">>> Loading samples file");
        
        Dataset<Row> sampleDF = SampleManager.readSamplesFile(GenotypeLevelFilterCommand.sampleFile);
        
        ///////////////////
        
        System.out.println(">>> Loading called variant data");
        
        Dataset<Row> cvDF = CalledVariantSparkUtils.getCalledVariantDF()
                .persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        System.out.print("\t> Called variants loaded: ");
        System.out.println(cvDF.count());
                
        ///////////////////

//        System.out.println(">>> Spliting data into blocks");
//
//        Dataset<Row> blockIdDF =
//                CalledVariantSparkUtils.getBlockIdDF(cvDF);
//        
//        int blockCount = (int) blockIdDF.count();
//
//        List<String> blockIds =
//                blockIdDF.toJavaRDD()
//                .map((Row r) -> r.getString(0))
//                .collect();
//        
//        int numIterations = blockIds.size()/STEPSIZE + ((blockIds.size()%STEPSIZE > 0) ? 1 : 0);
//        System.out.println(">>> Expected number of iterations: "+numIterations);
        
        ///////////////////

//        cvDF.unpersist();

        int blockBegin;
        int blockEnd;

//        for(int i=0;i<blockCount;i+=STEPSIZE) {
//            int begin = i;
//            int end = (i+STEPSIZE < blockCount) ? i+STEPSIZE : blockCount;

            // Build block sublist
//            List<String> filteredBlockIdsList = blockIdDFs[i].toJavaRDD()
//                    .map((Row r) -> r.getString(0))
//                    .collect();
//                    blockIds.subList(begin, end);

//            String[] filteredBlockIds =
//                filteredBlockIdsList.toArray(new String[filteredBlockIdsList.size()]);

//            String commaSepBlockIds =
//                    Utils.strjoin(filteredBlockIds,", ","'");
int i =0;
//            System.out.println(">>> Block iteration ["+begin+","+end+"[");
            System.out.println(">>> Block iteration "+Integer.toString(i));
//            System.out.print("\t> Blocks being analyzed: \033[90m");
//            System.out.println(commaSepBlockIds);
//            System.out.print("\033[m");
            /* */

            // Filter called_variants
            System.out.println("\t> Obtaining called variant subset");
            Dataset<Row> filteredCVDF =
                    cvDF;//.where(cvDF.col("block_id").isin((Object[])filteredBlockIds));
                    
            filteredCVDF = filteredCVDF.join(sampleDF,
                    filteredCVDF.col("sample_id").equalTo(sampleDF.col("id")),
                    "left").drop("id");
            
            filteredCVDF = CarrierSparkUtils.applyCarrierFilters(filteredCVDF);
            /* */
            
            /*  Build non-carriers list (chr, pos and sample_id) */
            System.out.println("\t> Building non-carriers list");
            Dataset<Row> allVarPosDF = CalledVariantSparkUtils.getVarChrPosDF(filteredCVDF);
            
            
            Dataset<Row> allSamplePosDF = 
                allVarPosDF.join(sampleDF, lit(true), "outer")
                    .withColumnRenamed("id", "sample_id");
            
            
            Dataset<Row> simpleVariantDF = filteredCVDF.
                    select("chr","pos","block_id","variant_id","sample_id","pheno");
            
            Dataset<Row> nonCarrierPosDF = allSamplePosDF
                    .except(simpleVariantDF);
            /* */
                    


            /* Get read_coverage data from DB */
            System.out.println("\t> Loading read coverage data");
            Dataset<Row> readCoverageDF =
                    CalledVariantSparkUtils.getReadCoverageDF(null);
            /* */
            

            /*  group non-carrier data and read_coverage by (sampl, block) */
            System.out.println("\t> Grouping non-carrier and coverage data");
            // Both nonCarrier and carrier data are keyed by "<block_id>-<sample_id>",
            //      so we can group with the read_coverage encoded string
            
            KeyValueGroupedDataset<String, Row> nonCarrierPosGroupedDF = nonCarrierPosDF.groupByKey(
                    (Row r) -> r.getString(2)+"-"+Integer.toString(r.getInt(4)),
                    Encoders.STRING());
            
            KeyValueGroupedDataset<String, Row> readCoverageGroupedDF = readCoverageDF.groupByKey(
                    (Row r) -> r.getString(0)+"-"+Integer.toString(r.getInt(1)),
                    Encoders.STRING());
            /* */
            
            System.out.println("\t> Decompressing coverage data");

            Dataset<Row> nonCarrierDataWithCoverageDF = nonCarrierPosGroupedDF.cogroup(readCoverageGroupedDF,
                    (String k, Iterator<Row> nonCarriers, Iterator<Row> rcIterator) -> {
                        TreeMap<Short,Short> tm = new TreeMap<>();
                        LinkedList<Row> l = new LinkedList<>();
                                
                        if(!rcIterator.hasNext()) {
                            return l.iterator();
                        }
                        Row rc = rcIterator.next();
                        
                        String[] covPieces = rc.getString(2)
                                .split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
                        
                        short pos = 0;
                        for(int inx=0;inx<covPieces.length;inx+=2) {
                            tm.put(pos, Utils.getCovValue(covPieces[inx+1].charAt(0)));
                            pos += Short.parseShort(covPieces[inx]);
                        }
                        
                        long blockStart = Long.parseLong(rc.getString(0).split("-")[1])*1024 + 1;
                        
                        while(nonCarriers.hasNext()) {
                            Row r = nonCarriers.next();
                            short offset = (short) (r.getInt(1) - blockStart);
                            l.add(RowFactory.create(
                                r.getString(0),r.getInt(1),r.getString(2),r.getInt(3),r.getInt(4),r.getShort(5),
                                    tm.floorEntry(offset).getValue()
                            ));
                        }
                        
                        return l.iterator();
                    },
                    RowEncoder.apply(nonCarrierPosDF.schema().add("coverage", DataTypes.ShortType)));
            
            // Apply coverage filters
            nonCarrierDataWithCoverageDF =
                    NonCarrierSparkUtils.applyCoverageFiltersToNonCarriers(nonCarrierDataWithCoverageDF);
            
            System.out.println("\t> Grouping variant data");

            // This time, nonCarrier and carrier data are keyed by variant_id,
            //      so we can group with the read_coverage encoded string
            
            KeyValueGroupedDataset<Integer, Row> nonCarrierDataGroupedDF = nonCarrierDataWithCoverageDF.groupByKey(
                    (Row r) -> r.getInt(3),
                    Encoders.INT());
            
            KeyValueGroupedDataset<Integer, Row> carrierDataGroupedDF = filteredCVDF.groupByKey(
                    (Row r) -> r.getInt(2),
                    Encoders.INT());
            
//            Dataset<CalledVariant> calledVariantDataset =
//                carrierDataGroupedDF.cogroup(nonCarrierDataGroupedDF, 
//                    (Integer vid, Iterator<Row> carriers, Iterator<Row> nonCarriers) -> {
//                        ArrayList<CalledVariant> l = new ArrayList<>(1);
//                        l.add(new CalledVariant(vid,carriers,nonCarriers));
//                        return l.iterator();
//                    },Encoders.kryo(CalledVariant.class));
//            
//
//            System.out.println("\t> Generating output");
//            Dataset<String> outputDataset = calledVariantDataset.flatMap(
//                    (CalledVariant cv) -> {
//                      VarGenoOutput out = new VarGenoOutput(cv);
//                      cv.addSampleDataToOutput(out);
//                      out.calculate();
//                      return out.getRowStrings().iterator();
//                    },
//                    Encoders.STRING());

            Dataset<Row> outputDataset =
                carrierDataGroupedDF.cogroup(nonCarrierDataGroupedDF, 
                    (Integer vid, Iterator<Row> carriers, Iterator<Row> nonCarriers) -> {
                        CalledVariant cv = new CalledVariant(vid,carriers,nonCarriers);
                        VarGenoOutput out = new VarGenoOutput(cv);
                        cv.addSampleDataToOutput(out);
                        out.calculate();
                        return out.getRows().iterator();
                    },
//                    Encoders.STRING());
                    RowEncoder.apply(VarGenoOutput.getSchema()));
            
            outputDataset = CalledVariantSparkUtils.applyOutputFilters(outputDataset);
            
            outputDataset = outputDataset.drop(VarGenoOutput.colsToBeDropped);
            
            outputDataset
                    .coalesce( PopSpark.session.sparkContext().defaultParallelism() )
                    .write()
                    .mode(i == 0 ? "overwrite" : "append")
                    .option("header", "true")
                    .option("nullValue", "NA")
                    .csv(CommonCommand.realOutputPath);
            

//            System.exit(-1);

            System.out.println();
            System.out.println();

//        }
    }
    
}
