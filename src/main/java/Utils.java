/**
 *
 * @author ferocha
 */
import java.util.StringJoiner;

public class Utils {
    public static String strjoin(String[] strArr, String delimiter) {
        StringJoiner sj = new StringJoiner(delimiter);
        for (String s : strArr)
            sj.add("'"+s+ "'");
        return sj.toString();
    }
}
