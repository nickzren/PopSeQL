package function.genotype.base;

import global.Index;
import utils.SparkManager;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import utils.ErrorManager;

/**
 *
 * @author nick
 */
public class SampleManager {

    private static HashMap<Integer, Byte> samplePhoneMap = new HashMap<>();

    private static int caseNum = 0;
    private static int ctrlNum = 0;

    public static void init() {
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

                if (!samplePhoneMap.containsKey(sampleId)) {
                    samplePhoneMap.put(sampleId, pheno);

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
    }

    public static HashMap<Integer, Byte> getSamplePhenoMap() {
        return samplePhoneMap;
    }

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
