package function.variant.base;

import global.Data;
import utils.FormatManager;

/**
 *
 * @author nick
 */
public class Region {

    protected String chrStr;
    protected int chrNum;
    protected int startPosition;
    protected int endPosition;
    protected int length;

    public Region() {
    }

    public Region(String chr, int start, int end) {
        initRegion(chr, start, end);
    }

    public void initRegion(String chr, int start, int end) {
        chrStr = chr;

        chrNum = intChr();

        startPosition = start;
        endPosition = end;

        length = endPosition - startPosition + 1;
    }

    private int intChr() {
        if (chrStr.equals("X")) {
            return 23;
        } else if (chrStr.equals("Y")) {
            return 24;
        } else if (chrStr.equals("MT")) {
            return 26;
        } else if (FormatManager.isInteger(chrStr)) {
            return Integer.parseInt(chrStr);
        } else {
            return Data.INTEGER_NA;
        }
    }

    public String getChrStr() {
        return chrStr;
    }

    public int getChrNum() {
        return chrNum;
    }

    public void setStartPosition(int start) {
        startPosition = start;
    }

    public void setEndPosition(int end) {
        endPosition = end;
    }

    public int getStartPosition() {
        return startPosition;
    }

    public int getEndPosition() {
        return endPosition;
    }

    public void setLength(int len) {
        length = len;
    }

    public int getLength() {
        return length;
    }
}
