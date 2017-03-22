package function.genotype.collapsing;

import function.genotype.base.Gene;
import function.genotype.base.GeneManager;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 */
public class VariantGeneFilterFunc implements FilterFunction<Row> {

     HashMap<String, TreeMap<Integer, Gene>> geneMap;
    
    public VariantGeneFilterFunc(){
        geneMap = GeneManager.getGeneMap();
    }
    
    @Override
    public boolean call(Row cvRow) throws Exception {
        String chr = cvRow.getString(2);
        int pos = cvRow.getInt(3);

        TreeMap<Integer, Gene> geneChrMap = geneMap.get(chr);

        if (geneChrMap != null) {
            Map.Entry<Integer, Gene> geneEntry = geneChrMap.floorEntry(pos);

            if (geneEntry != null) {
                Gene gene = geneEntry.getValue();

                if (gene.contains(pos)) {
                    return true;
                }
            }
        }

        return false;
    }
}
