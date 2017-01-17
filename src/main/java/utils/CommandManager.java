package utils;

//import function.annotation.base.AnnotationLevelFilterCommand;
import function.genotype.base.GenotypeLevelFilterCommand;
import global.Data;

//import function.genotype.statistics.StatisticsCommand;

import function.genotype.vargeno.VarGenoCommand;

//import function.variant.base.VariantLevelFilterCommand;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author nick
 */
public class CommandManager {

    private static String[] optionArray;
    private static ArrayList<CommandOption> optionList = new ArrayList<>();
    public static String command = "";
    private static String commandFile = "";

    private static void initCommand4Debug() {
        String cmd = "";

        optionArray = cmd.split("\\s+");
    }

    public static void initOptions(String[] options) {
        try {
            initCommandOptions(options);

            initOptionList();

            initOutput();

            initFunctionOptions();

            initSubOptions();

            initCommonOptions();

            initOptions4Debug();

            outputInvalidOptions();
        } catch (Exception e) {
            ErrorManager.send(e);
        }
    }

    private static void initCommandOptions(String[] options) {
        if (options.length == 0) {
            if (CommonCommand.isDebug) {
                initCommand4Debug();
            } else {
                System.out.println("\nError: without any input parameters to run ATAV. \n\nExit...\n");
                System.exit(0);
            }
        } else // init options from command file or command line
        {
            if (isCommandFileIncluded(options)) {
                initCommandFromFile();
            } else {
                optionArray = options;
            }
        }

        cleanUpOddSymbol();

        initCommand4Log();
    }

    private static void cleanUpOddSymbol() {
        for (int i = 0; i < optionArray.length; i++) {
            // below solve situation: dash hyphen or dash only
            optionArray[i] = optionArray[i].replaceAll("\\u2013", "--"); // en dash --> hyphen
            optionArray[i] = optionArray[i].replaceAll("\\u2014", "--"); // em dash --> hyphen
            optionArray[i] = optionArray[i].replace("---", "--");
        }
    }

    private static boolean isCommandFileIncluded(String[] options) {
        for (int i = 0; i < options.length; i++) {
            if (options[i].equals("--command-file")) {
                if (isFileExist(options[i + 1])) {
                    commandFile = options[i + 1];
                } else {
                    System.out.println("\nInvalid value '" + options[i + 1]
                            + "' for '--command-file' option.");
                    System.exit(0);
                }

                return true;
            }
        }

        return false;
    }

    private static void initCommandFromFile() {
        File f = new File(commandFile);

        String lineStr = "";
        String cmd = "";

        try {
            FileInputStream fstream = new FileInputStream(f);
            DataInputStream in = new DataInputStream(new FileInputStream(f));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            while ((lineStr = br.readLine()) != null) {
                cmd += lineStr + " ";
            }

            br.close();
            in.close();
            fstream.close();
        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
            ErrorManager.send(e);
        }

        optionArray = cmd.split("\\s+");
    }

    private static void initCommand4Log() {
        String version = Data.version;

        if (Data.version.contains(" ")) {
            version = Data.version.substring(Data.version.indexOf(" ") + 1);
        }

        command = "atav_" + version + ".sh";

        if (commandFile.isEmpty()) {
            for (String str : optionArray) {
                command += " " + str;
            }
        } else {
            command += " " + "--command-file " + commandFile;
        }
    }

    /*
     * init option list by user ATAV command
     */
    private static void initOptionList() {
        int valueIndex;

        for (int i = 0; i < optionArray.length; i++) {
            if (optionArray[i].startsWith("--")) {
                valueIndex = i + 1;

                try {
                    if (valueIndex == optionArray.length
                            || (optionArray[valueIndex].startsWith("-")
                            && !FormatManager.isDouble(optionArray[valueIndex]))) {
                        optionList.add(new CommandOption(optionArray[i], ""));
                    } else {
                        optionList.add(new CommandOption(optionArray[i], optionArray[++i]));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Invalid command option: " + optionArray[i]);
                System.exit(0);
            }
        }
    }

    /*
     * get output value from ATAV command then init output path
     */
    private static void initOutput() {
        Iterator<CommandOption> iterator = optionList.iterator();
        CommandOption option;

        while (iterator.hasNext()) {
            option = (CommandOption) iterator.next();
            if (option.getName().equals("--out")) {
                initOutputPath(option.getValue());
                iterator.remove();
                break;
            }
        }

        if (CommonCommand.outputPath.isEmpty()) {
            System.out.println("\nPlease specify output path: --out $PATH \n\nExit...\n");
            System.exit(0);
        }
    }

    private static void initOutputPath(String path) {
        try {
            CommonCommand.realOutputPath = path;
            CommonCommand.outputDirName = "output";
            CommonCommand.outputPath = path;
        } catch (Exception e) {
            System.out.println("\nError in creating an output folder, caused by " + e.toString() + " \n\nExit...\n");
            System.exit(0);
        }
    }

    private static void initOptions4Debug() {
        Iterator<CommandOption> iterator = optionList.iterator();
        CommandOption option;

        while (iterator.hasNext()) {
            option = (CommandOption) iterator.next();
            if (option.getName().equals("--db-host")) {
            } else if (option.getName().equals("--debug")) {
                CommonCommand.isDebug = true;
            } else {
                continue;
            }

            iterator.remove();
        }
    }

    private static void initFunctionOptions() throws Exception {
        Iterator<CommandOption> iterator = optionList.iterator();
        CommandOption option;
        boolean hasMainFunction = false;

        while (iterator.hasNext()) {
            option = (CommandOption) iterator.next();

            switch (option.getName()) {
                // Genotype Analysis Functions
                case "--list-var-geno":
                    VarGenoCommand.isListVarGeno = true;
                    break;
                default:
                    continue;
            }

            iterator.remove();
            hasMainFunction = true;
            break;
        }

        if (!hasMainFunction) {
            ErrorManager.print("Missing function command: --list-var-geno, --collapsing-dom, --collapsing-rec, "
                    + "--collapsing-comp-het, --fisher, --linear...");
        }
    }

    private static void initSubOptions() throws Exception {
        if (VarGenoCommand.isListVarGeno) { // Genotype Analysis Functions
            VarGenoCommand.initOptions(optionList.iterator());
        } 
    }

    private static void initCommonOptions() throws Exception {
//        VariantLevelFilterCommand.initOptions(optionList.iterator());

//        AnnotationLevelFilterCommand.initOptions(optionList.iterator());

        GenotypeLevelFilterCommand.initOptions(optionList.iterator());
    }

    public static void outputInvalidOptions() {
        Iterator<CommandOption> iterator = optionList.iterator();
        CommandOption option;

        boolean hasInvalid = false;

        while (iterator.hasNext()) {
            hasInvalid = true;

            option = (CommandOption) iterator.next();
            
            System.err.println("Invalid option: " + option.getName());

//            LogManager.writeAndPrint("Invalid option: " + option.getName());
        }

        if (hasInvalid) {
            ErrorManager.print("You have invalid options in your ATAV command.");
        }
    }

    /*
     * output invalid option & value if value > max or value < min ATAV stop
     */
    public static void checkValueValid(double max, double min, CommandOption option) {
        double value = Double.parseDouble(option.getValue());
        if (max != Data.NO_FILTER) {
            if (value > max) {
                outputInvalidOptionValue(option);
            }
        }

        if (min != Data.NO_FILTER) {
            if (value < min) {
                outputInvalidOptionValue(option);
            }
        }
    }

    /*
     * output invalid option & value if value is not in strList ATAV stop
     */
    public static void checkValueValid(String[] strList, CommandOption option) {
        for (String str : strList) {
            if (option.getValue().equals(str)) {
                return;
            }
        }

        outputInvalidOptionValue(option);
    }

    /*
     * output invalid option & value if value is not in strList ATAV stop
     */
    public static void checkValuesValid(String[] array, CommandOption option) {
        HashSet<String> set = new HashSet<>();

        set.addAll(Arrays.asList(array));

        String[] values = option.getValue().split(",");

        for (String str : values) {
            if (!set.contains(str)) {
                outputInvalidOptionValue(option);
            }
        }
    }

    /*
     * output invalid option & value if value is not a valid range
     */
    public static void checkRangeValid(String range, CommandOption option) {
        boolean isValid = false;

        String[] pos = range.split("-");
        double minStart = Double.valueOf(pos[0]);
        double maxEnd = Double.valueOf(pos[1]);

        if (option.getValue().contains("-")) {
            pos = option.getValue().split("-");
            double start = Double.valueOf(pos[0]);
            double end = Double.valueOf(pos[1]);

            if (start >= minStart && end <= maxEnd) {
                isValid = true;
            }
        }

        if (!isValid) {
            outputInvalidOptionValue(option);
        }
    }

    public static double[] getValidRange(CommandOption option) {
        double[] range = {0, 1};

        String[] pos = option.getValue().split("-");
        double start = Double.valueOf(pos[0]);
        double end = Double.valueOf(pos[1]);

        range[0] = start;
        range[1] = end;

        return range;
    }

    public static int getValidInteger(CommandOption option) {
        int i = 0;
        try {
            i = Integer.parseInt(option.getValue());
        } catch (NumberFormatException nfe) {
            outputInvalidOptionValue(option);
        }

        return i;
    }

    public static double getValidDouble(CommandOption option) {
        double i = 0;
        try {
            i = Double.parseDouble(option.getValue());
        } catch (NumberFormatException nfe) {
            outputInvalidOptionValue(option);
        }

        return i;
    }

    public static float getValidFloat(CommandOption option) {
        float i = 0;
        try {
            i = Float.parseFloat(option.getValue());
        } catch (NumberFormatException nfe) {
            outputInvalidOptionValue(option);
        }

        return i;
    }

    /*
     * output invalid option & value if value is a valid file path return either
     * a valid file path or a string value ATAV stop
     */
    public static String getValidPath(CommandOption option) {
        String path = option.getValue();

//        if (!isFileExist(path)) {
//            outputInvalidOptionValue(option);
//        }

        return path;
    }

    private static boolean isFileExist(String path) {
        if (path.isEmpty()) {
            return false;
        }

        if (path.contains(File.separator)) {
            File file = new File(path);
            if (!file.isFile()) {
                return false;
            }
        }

        return true;
    }

    public static void outputInvalidOptionValue(CommandOption option) {
        ErrorManager.print("\nInvalid value '" + option.getValue()
                + "' for '" + option.getName() + "' option.");
    }
}
