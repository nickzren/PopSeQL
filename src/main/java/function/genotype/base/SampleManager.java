package function.genotype.base;

import global.Index;
import global.PopSpark;
import global.Utils;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author nick
 */
public class SampleManager {
    
    public static Dataset<Row> sampleDF = null;
    
    public static String[] sampleIds;
    public static String commaSepSampleIds;
    
    private static int listSize; // case + ctrl
    private static int caseNum = 0;
    private static int ctrlNum = 0;
    
    public static Dataset<Row> readSamplesFile(String filepath) {
        sampleDF = PopSpark.session.read()
                .option("header", "false")
                .option("delimiter", "\t")
                .schema(
                        new StructType()
                        .add("id", DataTypes.IntegerType, false)
                        .add("pheno", DataTypes.ShortType, false)
                ).csv(filepath).cache();
        countCaseCtrl();
        sampleIds = SampleManager.getSampleIds();
        commaSepSampleIds = Utils.strjoin(sampleIds, ", ", "'");
        return sampleDF;
    }
    
    public static String[] getSampleIds(Dataset<Row> sampleDF) {
        List<String> sampleIdsList =
                sampleDF.toJavaRDD()
                .map((Row r) -> Integer.toString(r.getInt(0)))
                .collect();

        return sampleIdsList.toArray(new String[sampleIdsList.size()]);
    }
    
    public static String[] getSampleIds() {
        return getSampleIds(sampleDF);
    }
    
    public static void countCaseCtrl() {
        List<Row> l = sampleDF.groupBy("pheno").count().orderBy("pheno").collectAsList();
        ctrlNum = (int) l.get(Index.CTRL).getLong(1);
        caseNum = (int) l.get(Index.CASE).getLong(1);
        listSize = caseNum + ctrlNum;
        
        System.out.print("\tTotal samples: ");
        System.out.println(listSize);
        System.out.print("\tCase samples: ");
        System.out.println(caseNum);
        System.out.print("\tCtrl samples: ");
        System.out.println(ctrlNum);
        
    }
    
    public static int getListSize() {
        return listSize;
    }
    
    public static int getCaseNum() {
        return caseNum;
    }

    public static int getCtrlNum() {
        return ctrlNum;
    }
    
}