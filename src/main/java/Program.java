import function.genotype.vargeno.ListVarGeno;
import global.PopSpark;

/**
 *
 * @author nick
 */
public class Program {

    public static void main(String[] args) {
        
        PopSpark.init();

        ListVarGeno.run();

        PopSpark.destroy();
   }
}
