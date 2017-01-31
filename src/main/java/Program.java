
import function.genotype.base.GeneManager;
import function.genotype.base.SampleManager;
import function.genotype.collapsing.CollapsingCommand;
import function.genotype.collapsing.CollapsingSingleVariant;
import utils.SparkManager;
import utils.CommandManager;
import function.genotype.vargeno.ListVarGeno;
import function.genotype.vargeno.VarGenoCommand;

/**
 *
 * @author nick
 */
public class Program {

    public static void main(String[] args) {
        SparkManager.start();

        CommandManager.initOptions(args);

        SampleManager.init();

        GeneManager.init();

        if (VarGenoCommand.isListVarGeno) {
            ListVarGeno.run();
        } else if (CollapsingCommand.isCollapsingSingleVariant) {
            CollapsingSingleVariant.run();
        }

        SparkManager.stop();
    }
}
