
import function.genotype.base.SampleManager;
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

        if (VarGenoCommand.isListVarGeno) {
            ListVarGeno.run();
        }

        SparkManager.stop();
    }
}
