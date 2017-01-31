package function.genotype.base;

import global.Index;

/**
 *
 * @author nick
 */
public class Sample {

    private int id;
    private byte pheno; // ctrl 0 , case 1

    public Sample(int id, byte pheno) {
        this.id = id;
        this.pheno = pheno;
    }

    public boolean isCase() {
        return pheno == Index.CASE;
    }
}
