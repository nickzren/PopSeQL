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
                .config("spark.yarn.executor.memoryOverhead", "1024")
                .config("spark.executor.heartbeatInterval", "500s")
                .config("spark.network.timeout", "1000s")
                //                .config("spark.default.parallelism", "3000")
                .config("spark.driver.maxResultSize", "450g")
                .config("spark.sql.crossJoin.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", "utils.Registrator")
                .getOrCreate();
        SparkManager.context = new JavaSparkContext(session.sparkContext());

    }

    public static void stop() {
        SparkManager.session.stop();
    }
}
