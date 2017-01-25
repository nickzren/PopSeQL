package utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author felipe
 */
public class SparkManager {

    public static SparkSession session;
    public static JavaSparkContext context;    
    
    public static void start() {
        SparkManager.session = SparkSession.builder()
                .appName("PopSeQL")
                .config("spark.sql.crossJoin.enabled", "true")
                .config("spark.shuffle.spill.compress","false")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        SparkManager.context = new JavaSparkContext(session.sparkContext());
        
    }
    
    public static void stop() {
        SparkManager.session.stop();
    }
}
