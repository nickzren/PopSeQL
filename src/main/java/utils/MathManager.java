package utils;

import global.Data;
import java.math.BigDecimal;

/**
 *
 * @author nick, quanli
 */
public class MathManager {

    public static double devide(double a, double b) {
        if (b == 0 || b == Data.NA || a == Data.NA) {
            return Data.NA;
        } else {
            return a / b;
        }
    }

    public static double abs(double a, double b) {
        if (b == Data.NA || a == Data.NA) {
            return Data.NA;
        } else {
            return Math.abs(a - b);
        }
    }

    public static double roundToDecimals(double value) {
        int t = (int) (value * 100000000 + 0.5);
        double pValue = (double) t / 100000000;

        if (pValue > 0.00001) {
            return pValue;
        }

        BigDecimal temp = new BigDecimal(value);
        temp = temp.setScale(8, BigDecimal.ROUND_HALF_UP);

        return temp.doubleValue();
    }
}
