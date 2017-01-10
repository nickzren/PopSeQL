package utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author felipe
 */
public class PopSpark {

    public static SparkSession session;
    public static JavaSparkContext context;
    public static String jdbcURL = "jdbc:mysql://localhost:3306/annodb?user=test&password=test";
    
    
    public static void init() {
        PopSpark.session = SparkSession.builder()
                .appName("PopSeQL")
                .config("spark.sql.crossJoin.enabled", "true")
                .config("spark.shuffle.spill.compress","false")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        PopSpark.context = new JavaSparkContext(session.sparkContext());
        
    }
    
    public static void destroy() {
        PopSpark.session.stop();
    }
}
