package function.genotype.collapsing;

import global.Data;
import java.util.Iterator;
import static utils.CommandManager.checkValueValid;
import static utils.CommandManager.getValidDouble;
import utils.CommandOption;

/**
 *
 * @author nick
 */
public class CollapsingCommand {

    public static boolean isCollapsingSingleVariant = false;
    public static double maxLooMaf = Data.NO_FILTER;

    public static void initSingleVarOptions(Iterator<CommandOption> iterator)
            throws Exception { // collapsing dom or rec
        CommandOption option;

        while (iterator.hasNext()) {
            option = iterator.next();
            switch (option.getName()) {
                case "--loo-maf":
                    checkValueValid(0.5, 0, option);
                    maxLooMaf = getValidDouble(option);
                    break;
                default:
                    continue;
            }

            iterator.remove();
        }
    }

    public static boolean isMaxLooMafValid(double value) {
        if (maxLooMaf == Data.NO_FILTER) {
            return true;
        }

        return value <= maxLooMaf;
    }
}
