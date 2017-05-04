package function.genotype.collapsing;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 *
 * @author nick
 */
public class CovRegionFilterFunc implements FilterFunction<Row> {

    private String regionInput;

    public CovRegionFilterFunc(String input) {
        regionInput = input;
    }

    @Override
    public boolean call(Row cvRow) throws Exception {
        if (regionInput.isEmpty()) {
            return true;
        }

        String blockId = cvRow.getString(0);

        return blockId.split("-")[0].equals(regionInput);
    }
}
