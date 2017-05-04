package function.genotype.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.TreeMap;
import utils.ErrorManager;

/**
 *
 * @author nick
 */
public class GeneManager {

    private static HashMap<String, TreeMap<Integer, Gene>> geneMap = new HashMap<>();

    public static void init() {
        if (AnnotationLevelFilterCommand.geneInput.isEmpty()) {
            return;
        }

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

                    if (!AnnotationLevelFilterCommand.regionInput.isEmpty()
                            && !chr.equals(AnnotationLevelFilterCommand.regionInput)) {
                        continue;
                    }

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
    }

    public static HashMap<String, TreeMap<Integer, Gene>> getGeneMap() {
        return geneMap;
    }
}
