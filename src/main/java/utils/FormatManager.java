package utils;

import global.Data;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 *
 * @author nick
 */
public class FormatManager {

    private static NumberFormat pformat1 = new DecimalFormat("0.####");
    private static NumberFormat pformat2 = new DecimalFormat("0.###E000");
    private static NumberFormat pformat3 = new DecimalFormat("0.######");

    public static String getDouble(double value) {
        if (value == Data.DOUBLE_NA) {
            return Data.STRING_NA;
        }

        if (value < 0.001 && value > 0) {
            return pformat2.format(value);
        } else {
            return pformat1.format(value);
        }
    }   

    public static String getFloat(float value) {
        if (value == Data.FLOAT_NA) {
            return Data.STRING_NA;
        }

        if (value == 0.0f) {
            return "0";
        }

        if (value < 0.001 && value > 0) {
            return pformat2.format(value);
        } else {
            return pformat1.format(value);
        }
    }
    
    public static String getByte(byte value) {
        if (value == Data.BYTE_NA) {
            return Data.STRING_NA;
        }

        return String.valueOf(value);
    }

    public static String getShort(short value) {
        if (value == Data.SHORT_NA) {
            return Data.STRING_NA;
        }

        return String.valueOf(value);
    }

    public static String getInteger(int value) {
        if (value == Data.INTEGER_NA || value == Data.SHORT_NA) {
            return Data.STRING_NA;
        }

        return String.valueOf(value);
    }

    public static boolean isDouble(String input) {
        try {
            Double.parseDouble(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isInteger(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
