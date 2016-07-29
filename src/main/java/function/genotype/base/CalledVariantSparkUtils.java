package function.genotype.base;

import global.PopSpark;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author felipe
 */
public class CalledVariantSparkUtils {

    public static Dataset<Row> getCalledVariantDF() {
        return PopSpark.session.read().jdbc(PopSpark.jdbcURL, "( select * from called_variant ) t1", new Properties());
    }

    public static Dataset<Row> getSampleIdDF(Dataset<Row> cvDF) {
        return PopSpark.session.read().jdbc(PopSpark.jdbcURL, "( select distinct sample_id from read_coverage ) t1", new Properties()).select("sample_id").distinct();
    }

    public static Dataset<Row> getVarChrPosDF(Dataset<Row> cvDF) {
        return cvDF.select("chr", "pos", "block_id", "variant_id").distinct();
    }

    public static Dataset<Row> getBlockIdDF(Dataset<Row> cvDF) {
        return cvDF.select("block_id").distinct();
    }
    
    //    public static Dataset<Row> getCarrierDF( Dataset<Row> posSampleCrossDF, Dataset<Row> cvDF) {
//        return posSampleCrossDF.join(cvDF,
//                    cvDF.col("sample_id").equalTo(posSampleCrossDF.col("p_sample_id"))
//                    .and(cvDF.col("chr").equalTo(posSampleCrossDF.col("p_chr")))
//                    .and(cvDF.col("pos").equalTo(posSampleCrossDF.col("p_pos"))),
//            "left");
//    }
    
}
