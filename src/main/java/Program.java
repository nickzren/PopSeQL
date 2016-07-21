import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
/**
 *
 * @author nick
 */
public class Program {

    public static void main(String[] args) {
            //String logFile = "/usr/local/Cellar/apache-spark/1.6.1/sparkTestLog.txt";
    String url = "jdbc:mysql://localhost:3306/annodb?user=test&password=test";       
    SparkConf conf = new SparkConf().setAppName("Example of Spark SQL Connection"); // Set this on commandline  .setMaster("spark://igm-it-spare.local:7077");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.addJar("/Users/kaustubh/NetBeansProjects/SparkMySQLTest/target/SparkMySQLTest-1.0-SNAPSHOT.jar");
    SQLContext cont = new SQLContext(sc);
    //Registering Driver with main() method
    //Class.forName("com.mysql.jdbc.Driver").newInstance();
    
    //Loading Options
    Map<String, String> options = new HashMap<>();
    options.put("url", url);
    options.put("dbtable", "annodb.read_coverage");
    options.put("driver","com.mysql.jdbc.Driver");
    DataFrame readCoverage = cont.load("jdbc", options);            
    String [] cols=readCoverage.columns();
    for (String i: cols)
       System.out.println("cols="+i);
    Row [] testRes =readCoverage.where(cols[1]+"=38671").select(readCoverage.col(cols[2])).collect();
    for (Row i : testRes){
        System.out.println("Sparksqltest.main() -->"+i.toString());
    }
   }
}
