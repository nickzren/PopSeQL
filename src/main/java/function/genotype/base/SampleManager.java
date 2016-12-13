package function.genotype.base;

import global.Index;
import utils.PopSpark;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.spark.broadcast.Broadcast;
import utils.ErrorManager;

/**
 *
 * @author nick
 */
public class SampleManager {

    private static Broadcast<HashMap<Integer, Integer>> sampleMapBroadcast;

    private static int caseNum = 0;
    private static int ctrlNum = 0;

    public static void init() {
        HashMap<Integer, Integer> sampleMap = new HashMap<>();

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
                int pheno = Integer.valueOf(values[1]);

                if (!sampleMap.containsKey(sampleId)) {
                    sampleMap.put(sampleId, pheno);

                    if (pheno == Index.CTRL) {
                        ctrlNum++;
                    } else if (pheno == Index.CASE) {
                        caseNum++;
                    }
                }

                br.close();
                in.close();
                fstream.close();
            }
        } catch (Exception e) {
            ErrorManager.send(e);
        }

        sampleMapBroadcast = PopSpark.context.broadcast(sampleMap);
    }

    public static Broadcast<HashMap<Integer, Integer>> getSampleMapBroadcast() {
        return sampleMapBroadcast;
    }

    public static int getCaseNum() {
        return caseNum;
    }

    public static int getCtrlNum() {
        return ctrlNum;
    }
}
