package function.genotype.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.spark.broadcast.Broadcast;
import utils.ErrorManager;
import utils.SparkManager;

/**
 *
 * @author nick
 */
public class GeneManager {

    private static Broadcast<HashMap<String, TreeMap<Integer, Gene>>> geneMapBroadcast;

    public static void init() {
        if (AnnotationLevelFilterCommand.geneInput.isEmpty()) {
            return;
        }

        HashMap<String, TreeMap<Integer, Gene>> geneMap = new HashMap<>();

        try {
            File file = new File(AnnotationLevelFilterCommand.geneInput);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                line = line.replaceAll("( )+", "");

                if (!line.isEmpty()) {
                    String[] tmp = line.split("\t"); // chr, start, end, gene name

                    String chr = tmp[0];
                    int start = Integer.valueOf(tmp[1]);
                    int end = Integer.valueOf(tmp[2]);
                    String geneName = tmp[3];

                    TreeMap<Integer, Gene> startPosGeneMap = geneMap.get(chr);

                    if (startPosGeneMap == null) {
                        startPosGeneMap = new TreeMap<>();
                        geneMap.put(chr, startPosGeneMap);
                    }

                    startPosGeneMap.put(start, new Gene(chr, start, end, geneName));
                }
            }
            br.close();
            fr.close();
        } catch (Exception ex) {
            ErrorManager.send(ex);
        }

        geneMapBroadcast = SparkManager.context.broadcast(geneMap);
    }

    public static Broadcast<HashMap<String, TreeMap<Integer, Gene>>> getGeneMapBroadcast() {
        return geneMapBroadcast;
    }
}
