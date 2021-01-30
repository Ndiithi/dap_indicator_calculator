/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.healthit.mohdap.indicator_calculator;

import com.healthit.mohdap.indicator_calculator.service.Aggregator;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author duncanndiithi
 */
public class Entry {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(Entry.class.getCanonicalName());

    private static Options options = new Options();
    private static final String helpString = "Indicator caculation module accepts csv file for indicators to process. If none is provided the module reads"
            + " all indicators fro the KHIS db and processes them for all organisation units and periods.\n"
            + "\tThe module operates in two modes:\n"
            + "\t\t Mode 1. Running specific indicators for a given period and/or orgunit/orglevel only.\n"
            + "\t\t Mode 2. Running all indicators from the KHIS database. This is the default mode for the app.";

    static {
        //Declare CLI options
        options.addOption("h", "help", false, "Display help information.")
                .addOption("i", "info", false, "Display app info.")
                .addOption("c", "continue", true, "Used on mode 2. Will pick processing from where it left off. Input is either yes/no. Note that this option cosumes alot of space during processing.")
                .addOption("from", "from-date", true, "Used on mode 2. if passed, the indicators processing will start from this date. Format (yyyy-mm-dd)")
                .addOption("to", "to-date", true, "Used on mode 2. if passed, the indicators processing period will not go beyond this date. Format (yyyy-mm-dd)")
                .addOption("level", "level", true, "Org level(1,2,3,4,5) to process given indicators from CSV. Active only when csv has indicators column alone.")
                .addOption("in", "input", true, "Used with mode 1. Path of file to use for in CVS format.")
                .addOption("out", "output", true, "Location to write csv file of calculated indicators. Ensure app has write privileges.\n"
                        + "If none is given, it output in current app directory");

        String log4jConfigFile = "./log4j.properties";
        PropertyConfigurator.configure(log4jConfigFile);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            String outputFilePath = null;
            String fromDate = null;
            String toDate = null;
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                System.out.println("DAP indicator calculation module.");
                System.exit(0);
            }

            if (cmd.hasOption("c") && cmd.hasOption("in")) {
                System.out.println("Cannot run both modes at same time. Choose to either run all indicators\n or give a list of "
                        + "indicators to run. Run app with -h for help.");
                System.exit(0);
            }

            if (cmd.hasOption("h")) {
                System.out.println("\n" + helpString + "\n");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("java -jar indicator_calculator.jar [options]", options);
                System.exit(0);
            }

            if (cmd.hasOption("out")) {
                outputFilePath = cmd.getOptionValue("out");
            }

            if (cmd.hasOption("from")) {
                fromDate = cmd.getOptionValue("from");
                log.debug("from date: " + fromDate);
            }
            if (cmd.hasOption("to")) {
                toDate = cmd.getOptionValue("to");
                log.debug("from date: " + toDate);
            }
            if (cmd.hasOption("in")) {
                String inputFilePath = cmd.getOptionValue("in");
                if (inputFilePath == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("java -jar indicator_calculator.jar [options]", options);
                    System.exit(0);
                } else {
                    processIndicatorsFromUserFile(inputFilePath, outputFilePath, cmd);
                    System.exit(0);
                }
            }

            boolean proceed = false;
            if (cmd.hasOption("c")) {

                String cont = cmd.getOptionValue("c");
                if (cont != null) {
                    if ("yes".contentEquals(cont.trim())) {
                        proceed = true;

                    }
                }
            }
            Aggregator.processAllIndicators(proceed, outputFilePath, fromDate, toDate);

        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void processIndicatorsFromUserFile(String inputFilePath, String outputFilePath, CommandLine cmd) {
        log.debug("File location: " + inputFilePath);
        Reader in = null;
        List<List<String>> indicatorsToProcess = null;
        boolean isProcesssByOrgLevel = false;
        boolean isProcesssIndicatorsOnly = false;
        try {
            in = new FileReader(inputFilePath);

            CSVParser csvParser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Iterable<CSVRecord> records = csvParser;
            List<String> headerNames = csvParser.getHeaderNames();
            if (!headerNames.contains("indicator")) {
                log.error("Your csv does not have an indicator column. Run app with -h for help.");
                System.exit(0);
            }
            if (headerNames.contains("organisation_level")) {
                isProcesssByOrgLevel = true;
            }
            if (headerNames.contains("organisation_level") || headerNames.contains("organisation")) {
                indicatorsToProcess = getValuesByOrgunitsFromCsvFile(records, isProcesssByOrgLevel);
            } else {

                indicatorsToProcess = getValuesByIndicatorsOnlysFromCsvFile(records);
                isProcesssIndicatorsOnly = true;
            }

            if (isProcesssIndicatorsOnly) {
                String fromDate = null;
                String toDate = null;
                if (!cmd.hasOption("level")) {
                    System.out.println("Please provide org level to calculate given indicators. Run app with -h for help.");
                    log.info("Please provide org level to calculate given indicators. Run app with -h for help.");
                    System.exit(0);
                }
                if (cmd.hasOption("from")) {
                    fromDate = cmd.getOptionValue("from");
                    log.debug("from date: " + fromDate);
                }
                if (cmd.hasOption("to")) {
                    toDate = cmd.getOptionValue("to");
                    log.debug("from date: " + toDate);
                }
                boolean proceed = false;
                if (cmd.hasOption("c")) {
                    String cont = cmd.getOptionValue("c");
                    if (cont != null) {
                        if ("yes".contentEquals(cont.trim())) {
                            proceed = true;
                        }
                    }
                }
                int level = Integer.parseInt(cmd.getOptionValue("level"));
                Aggregator.processByOrgLevel(indicatorsToProcess, level, fromDate, toDate, proceed, outputFilePath);
            } else {

                if (isProcesssByOrgLevel) {
                    Aggregator.processByOrgLevel(indicatorsToProcess, outputFilePath);
                } else {
                    Aggregator.processByOrgUnit(indicatorsToProcess, outputFilePath);
                }
            }

        } catch (FileNotFoundException ex) {
            log.error("File not found: " + inputFilePath);
        } catch (IOException ex) {
            log.error("Unable to read CSV file, check if correct format.  Run app with -h for help.");
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                log.error(ex);
            }
        }
    }

    private static List<List<String>> getValuesByOrgunitsFromCsvFile(Iterable<CSVRecord> records, boolean isProcesssByOrgLevel) {
        List<List<String>> indicatorsToProcess = new ArrayList();
        int csvLineNumber = 1;
        for (CSVRecord record : records) {
            try {
                csvLineNumber += 1;
                String indicator = record.get("indicator").trim();
                String organisation;
                if (isProcesssByOrgLevel) {
                    organisation = record.get("organisation_level").trim();
                } else {
                    organisation = record.get("organisation").trim();
                }

                String period = record.get("period");
                Date fromPeriod = new SimpleDateFormat("yyyy-MM-dd").parse(period);
                List params = new ArrayList();
                params.add(indicator);
                params.add(organisation);
                params.add(period);
                indicatorsToProcess.add(params);
            } catch (java.text.ParseException ex) {
                log.error("Unable to parse date at row no: " + csvLineNumber);
                System.exit(0);
            }
        }
        return indicatorsToProcess;
    }

    private static List<List<String>> getValuesByIndicatorsOnlysFromCsvFile(Iterable<CSVRecord> records) {
        List<List<String>> indicatorsToProcess = new ArrayList();
        for (CSVRecord record : records) {
            String indicator = record.get("indicator").trim();
            List params = new ArrayList();
            params.add(indicator);
            indicatorsToProcess.add(params);
        }
        return indicatorsToProcess;
    }

}
