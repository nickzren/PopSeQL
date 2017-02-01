package function.genotype.base;

import global.Index;
import utils.SparkManager;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.spark.broadcast.Broadcast;
import utils.ErrorManager;

/**
 *
 * @author nick
 */
public class SampleManager {

    private static Broadcast<HashMap<Integer, Byte>> sampleMapBroadcast;
//    private static Broadcast<ArrayList<Sample>> sampleListBroadcast;

    private static int caseNum = 0;
    private static int ctrlNum = 0;

    public static void init() {
        HashMap<Integer, Byte> sampleMap = new HashMap<>();
        ArrayList<Sample> sampleList = new ArrayList<>();

        String lineStr = "";

        try {
            File f = new File(GenotypeLevelFilterCommand.sampleFile);
            FileInputStream fstream = new FileInputStream(f);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            while ((lineStr = br.readLine()) != null) {

                if (lineStr.isEmpty()) {
                    continue;
                }

                lineStr = lineStr.replaceAll("( )+", "");

                String[] values = lineStr.split("\t");

                int sampleId = Integer.valueOf(values[0]);
                byte pheno = Byte.valueOf(values[1]);

                if (!sampleMap.containsKey(sampleId)) {
                    sampleMap.put(sampleId, pheno);
                    sampleList.add(new Sample(sampleId, pheno));

                    if (pheno == Index.CTRL) {
                        ctrlNum++;
                    } else if (pheno == Index.CASE) {
                        caseNum++;
                    }
                }
            }

            br.close();
            in.close();
            fstream.close();
        } catch (Exception e) {
            ErrorManager.send(e);
        }

        if (SparkManager.context == null) {
            System.out.println("PopSpark.context is null");
        }

        sampleMapBroadcast = SparkManager.context.broadcast(sampleMap);
//        sampleListBroadcast = SparkManager.context.broadcast(sampleList);
    }

    public static Broadcast<HashMap<Integer, Byte>> getSampleMapBroadcast() {
        return sampleMapBroadcast;
    }
    
//    public static Broadcast<ArrayList<Sample>> getSampleListBroadcast() {
//        return sampleListBroadcast;
//    }

    public static int getCaseNum() {
        return caseNum;
    }

    public static int getCtrlNum() {
        return ctrlNum;
    }

    public static int getSampleNum() {
        return caseNum + ctrlNum;
    }
}
