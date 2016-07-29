package global;

import org.apache.spark.sql.SparkSession;

/**
 *
 * @author felipe
 */
public class PopSpark {

    public static SparkSession session;
    public static String jdbcURL = "jdbc:mysql://localhost:3306/annodb?user=test&password=test";
    
    
    public static void init() {
        PopSpark.session = SparkSession.builder()
                .appName("PopSeQL")
                .config("spark.sql.crossJoin.enabled", "true")
                .getOrCreate();
    }
    
    public static void destroy() {
        PopSpark.session.stop();
    }
}
