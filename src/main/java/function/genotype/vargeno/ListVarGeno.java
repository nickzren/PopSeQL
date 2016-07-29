package function.genotype.vargeno;

import function.genotype.base.CalledVariant;
import function.genotype.base.CalledVariantSparkUtils;
import global.PopSpark;
import global.Utils;
import java.util.ArrayList;
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
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;

/**
 *
 * @author felipe
 */
public class ListVarGeno {
    
    public static final int STEPSIZE = 100000;
    
    public static Dataset<Row> readSamplesFile(String filepath) {
        return PopSpark.session.read()
                .option("header", "false")
                .schema(new StructType()
                    .add("sample", DataTypes.IntegerType, false)
                    .add("type", DataTypes.StringType, false))
                .csv(filepath);
    }
    
    public static void run() {
        System.out.println(">>> Loading called variant data");
        Dataset<Row> cvDF = CalledVariantSparkUtils.getCalledVariantDF()
                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        Dataset<Row> sampleIdDF = CalledVariantSparkUtils.getSampleIdDF(cvDF);

        List<String> sampleIdsList = 
                sampleIdDF.toJavaRDD()
                .map((Row r) -> Integer.toString(r.getInt(0)))
                .collect();

        String[] sampleIds = sampleIdsList.toArray(new String[sampleIdsList.size()]);


        System.out.println(">>> Spliting data into blocks");

        Dataset<Row> blockIdDF =
                CalledVariantSparkUtils.getBlockIdDF(cvDF);

        List<String> blockIds =
                blockIdDF.toJavaRDD()
                .map((Row r) -> r.getString(0))
                .collect();
        
        int numIterations = blockIds.size()/STEPSIZE + ((blockIds.size()%STEPSIZE > 0) ? 1 : 0);
        System.out.println(">>> Expected number of iterations: "+numIterations);

//        cvDF.unpersist();

        for(int i=0;i<blockIds.size();i+=STEPSIZE) {
            int begin = i;
            int end = (i+STEPSIZE < blockIds.size()) ? i+STEPSIZE : blockIds.size();

            // Build block sublist
            List<String> filteredBlockIdsList =
                    blockIds.subList(begin, end);

            String[] filteredBlockIds =
                filteredBlockIdsList.toArray(new String[filteredBlockIdsList.size()]);

            String commaSepBlockIds =
                    Utils.strjoin(filteredBlockIds,", ","'");

            System.out.println(">>> Block iteration ["+begin+","+end+"[");
//            System.out.print("\t> Blocks being analyzed: \033[90m");
//            System.out.println(commaSepBlockIds);
//            System.out.print("\033[m");
            /* */

            // Filter called_variants
            System.out.println("\t> Obtaining called variant subset");
            Dataset<Row> filteredCVDF =
                    cvDF.where(cvDF.col("block_id").isin((Object[])filteredBlockIds));
            /* */
            
            /*  Build non-carriers list (chr, pos and sample_id) */
            System.out.println("\t> Building non-carriers list");
            Dataset<Row> allVarPosDF = CalledVariantSparkUtils.getVarChrPosDF(filteredCVDF);
            
//            System.out.println(allPosDF.count());
//            System.out.println(sampleIdDF.count());
            
            Dataset<Row> allSamplePosDF = 
                allVarPosDF.join(sampleIdDF, lit(true), "outer");
            
//            System.out.println(allSamplePosDF.count());
            
            Dataset<Row> simpleVariantDF = filteredCVDF.
                    select("chr","pos","block_id","variant_id","sample_id");
//                    .withColumnRenamed("chr", "v_chr")
//                    .withColumnRenamed("pos", "v_pos")
//                    .withColumnRenamed("sample_id", "v_sample_id");
            
            Dataset<Row> nonCarrierPosDF = allSamplePosDF
                    .except(simpleVariantDF).cache();
//                    .join(simpleVariantDF,
//                        allSamplePosDF.col("chr").equalTo(simpleVariantDF.col("v_chr"))
//                        .and(allSamplePosDF.col("pos").equalTo(simpleVariantDF.col("v_pos")))
//                        .and(allSamplePosDF.col("sample_id").equalTo(simpleVariantDF.col("v_sample_id"))),
//                    "left")
//                    .where("variant_id IS NULL")
//                    .select("chr","pos","block_id","sample_id").cache();
            /* */
                    
//            simpleVariantDF.sort("v_chr","v_pos").show(100);
//            nonCarrierPosDF.sort("chr","pos").show(100);
//            System.out.println(nonCarrierPosDF.count());
            
//            System.out.println(simpleVariantDF.count());
//            System.out.println(nonCarrierPosDF.count());

            /* Get read_coverage data from DB */
            System.out.println("\t> Loading read coverage data");
            Dataset<Row> readCoverageDF =
                    PopSpark.session.read().jdbc(PopSpark.jdbcURL,
                        "( select * from read_coverage\n" +
                            "where block_id IN ( "+commaSepBlockIds+" )\n" +
                            "and sample_id IN ( "+Utils.strjoin(sampleIds,", ","'")+" ) ) t1",
                        new Properties());
            /* */
            
//            readCoverageDF.show();

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
                                r.getString(0),r.getInt(1),r.getString(2),r.getInt(3),r.getInt(4),
                                    tm.floorEntry(offset).getValue()
                            ));
                        }
                        
                        return l.iterator();
                    },
                    RowEncoder.apply(nonCarrierPosDF.schema().add("coverage", DataTypes.ShortType)));
            
            
//            nonCarrierDataWithCoverageDF.show(100);
            // This time, nonCarrier and carrier data are keyed by variant_id,
            //      so we can group with the read_coverage encoded string
            
            KeyValueGroupedDataset<Integer, Row> nonCarrierDataGroupedDF = nonCarrierDataWithCoverageDF.groupByKey(
                    (Row r) -> r.getInt(3),
                    Encoders.INT());
            
            KeyValueGroupedDataset<Integer, Row> carrierDataGroupedDF = filteredCVDF.groupByKey(
                    (Row r) -> r.getInt(2),
                    Encoders.INT());
            
            
            // TODO: build Variant instances
            // The cogroup() call below will handle each variant,
            //       mapping data to a CalledVariant instance
            // i.e. each variant will be inputed as
            //       (List<non-carrier>, List<carrier>)
            //       and outputed as some instance
            
            Dataset<CalledVariant> calledVariantDataset =
                carrierDataGroupedDF.cogroup(nonCarrierDataGroupedDF, 
                    (Integer vid, Iterator<Row> carriers, Iterator<Row> nonCarriers) -> {
                        ArrayList<CalledVariant> l = new ArrayList<>(1);
                        l.add(new CalledVariant(vid,carriers,nonCarriers));
                        return l.iterator();
                    },Encoders.kryo(CalledVariant.class));
            
//            List<CalledVariant> cvList = calledVariantDataset.collectAsList();
            
//            System.out.print("Total CalledVariant Instances: ");
//            System.out.println(cvList.size());

            Dataset<String> outputDataset = calledVariantDataset.flatMap(
                    (CalledVariant cv) -> cv.getStringRowIterator(),
                    Encoders.STRING());
            
            outputDataset
                    .coalesce(PopSpark.session.sparkContext().defaultParallelism())
                    .write()
                    .mode(i == 0 ? "overwrite" : "append")
                    .text("file:///Users/ferocha/igm/PopSeQL/target/output");
            
//            for (CalledVariant cv : cvList) {
//                
//            }
            

//            System.exit(-1);

            nonCarrierPosDF.unpersist();
            System.out.println();
            System.out.println();

        }
    }
    
}
