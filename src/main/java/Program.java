import global.PopSpark;
import utils.CommandManager;
import function.genotype.vargeno.ListVarGeno;
import function.genotype.vargeno.VarGenoCommand;

/**
 *
 * @author nick
 */
public class Program {

    public static void main(String[] args) {
        
        CommandManager.initOptions(args);
        
        PopSpark.init();

        if(VarGenoCommand.isListVarGeno)
            ListVarGeno.run2();

        PopSpark.destroy();
   }
}
