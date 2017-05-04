package function.genotype.base;

import java.util.Iterator;
import utils.CommandOption;

/**
 *
 * @author nick
 */
public class AnnotationLevelFilterCommand {

    public static String geneInput = "";
    public static String geneBoundaryFile = "";
    public static String regionInput = "";

    public static void initOptions(Iterator<CommandOption> iterator)
            throws Exception {
        CommandOption option;

        while (iterator.hasNext()) {
            option = iterator.next();
            switch (option.getName()) {
                case "--gene":
                    geneInput = option.getValue();
                    break;
                case "--gene-boundary":
                    geneBoundaryFile = option.getValue();
                    break;
                case "--region":
                    regionInput = option.getValue();
                    break;
                default:
                    continue;
            }

            iterator.remove();
        }
    }
}
