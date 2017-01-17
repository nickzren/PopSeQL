package function.variant.base;

import global.Data;
import utils.FormatManager;

/**
 *
 * @author nick
 */
public class Region implements Comparable {

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
        if (chrStr.equals("X")
                || chrStr.equals("XY")) {
            return 23;
        } else if (chrStr.equals("Y")) {
            return 24;
        } else if (chrStr.equals("MT")) {
            return 26;
        } else if (FormatManager.isInteger(chrStr)) {
            return Integer.parseInt(chrStr);
        } else {
            return Data.NA;
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

    @Override
    public String toString() {
        String chr = "chr" + chrStr;
        if (startPosition == Data.NA && endPosition == Data.NA) {
            return chr;
        }

        chr += ":" + startPosition + "-" + endPosition;

        return chr;
    }

    @Override
    public int compareTo(Object another) throws ClassCastException {
        Region that = (Region) another;
        return Double.compare(this.chrNum, that.chrNum); //small -> large
    }
}
