package function.genotype.base;

import java.io.Serializable;

/**
 *
 * @author nick
 */
public class Gene implements Serializable {

    private String name;
    private String chr;
    private int start;
    private int end;

    public Gene(String chr, int start, int end, String name) {
        this.chr = chr;
        this.start = start;
        this.end = end;
        this.name = name;
    }

    public String getChr() {
        return chr;
    }

    public String getName() {
        return name;
    }

    public boolean contains(String chr, int pos) {
        return chr.equals(chr)
                && pos >= start
                && pos <= end;
    }

    public boolean contains(int pos) {
        return pos <= end;
    }

    @Override
    public String toString() {
        return name;
    }
}
