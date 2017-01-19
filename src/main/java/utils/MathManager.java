package utils;

import global.Data;
import java.math.BigDecimal;

/**
 *
 * @author nick, quanli
 */
public class MathManager {

    public static double devide(double a, int b) {
        if (b == 0 || a == Data.DOUBLE_NA || b == Data.INTEGER_NA) {
            return Data.DOUBLE_NA;
        } else {
            return a / (double) b;
        }
    }

    public static float devide(float a, int b) {
        if (b == 0 || a == Data.FLOAT_NA || b == Data.INTEGER_NA) {
            return Data.FLOAT_NA;
        } else {
            return a / (float) b;
        }
    }

    public static float devide(int a, int b) {
        if (b == 0 || a == Data.INTEGER_NA || b == Data.INTEGER_NA) {
            return Data.FLOAT_NA;
        } else {
            return (float) a / (float) b;
        }
    }

    public static float devide(short a, short b) {
        if (b == 0 || a == Data.SHORT_NA || b == Data.SHORT_NA) {
            return Data.FLOAT_NA;
        } else {
            return (float) a / (float) b;
        }
    }

    public static float abs(float a, float b) {
        if (b == Data.FLOAT_NA || b == Data.INTEGER_NA
                || a == Data.FLOAT_NA || a == Data.INTEGER_NA) {
            return Data.FLOAT_NA;
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
