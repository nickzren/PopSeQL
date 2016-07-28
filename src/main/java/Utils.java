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
    
    public static short getCovValue(char letter) {
        switch (letter) {
            case 'a':
            default:
                return -1;
            case 'b':
                return 3;
            case 'c':
                return 10;
            case 'd':
                return 20;
            case 'e':
                return 201;
        }
    }
}
