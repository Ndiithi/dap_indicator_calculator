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
            + "\t\t Mode 1. Running specific indicators for a given period and orgunit only.\n"
            + "\t\t Mode 2. Running all indicators from the KHIS database. This resumes from last stopped preocessing timestamps for each "
            + "indicator. This is the default mode for the app.";

    static {
        //Declare CLI options
        options.addOption("c", "continue", true, "Will pick processing from where it left off.").
                addOption("h", "help", false, "Display help information.")
                .addOption("i", "info", false, "Display app info.")
                .addOption("in", "input", true, "Path of file to use for in CVS format.")
                .addOption("out", "output", true, "Location to write csv file of calculated indicators. Ensure app has write privileges.\n"
                        + "If none is given, it output in current app directory");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                System.out.println("DAP indicator calculation module.");
                System.exit(0);
            }

            if (cmd.hasOption("c") && cmd.hasOption("in")) {
                System.out.println("Cannot run both modes at same time. Choose to either run all indicators\n or give a list of "
                        + "indicators to run.");
                System.exit(0);
            }

            if (cmd.hasOption("h")) {
                System.out.println(helpString);
                System.exit(0);
            }

            if (cmd.hasOption("in")) {
                String inputFilePath = cmd.getOptionValue("in");
                String outputFilePath = cmd.getOptionValue("out");
                if (inputFilePath == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("java -jar indicator_calculator.jar [options]", options);
                    System.exit(0);
                } else {
                    processIndicatorsFromUserFile(inputFilePath, outputFilePath);
                }
            }

            Aggregator.processAllIndicators();

        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void processIndicatorsFromUserFile(String inputFilePath, String outputFilePath) {
        Reader in = null;
        List<List<String>> indicatorsToProcess = new ArrayList();
        int csvLineNumber = 1;
        boolean isProcesssByOrgLevel = false;
        try {
            in = new FileReader("inputFilePath");

            CSVParser csvParser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Iterable<CSVRecord> records = csvParser;
            List<String> headerNames = csvParser.getHeaderNames();
            if (headerNames.contains("organisation_level")) {
                isProcesssByOrgLevel = true;
            }

            for (CSVRecord record : records) {
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
            }
            if (isProcesssByOrgLevel) {
                Aggregator.processByOrgLevel(indicatorsToProcess, outputFilePath);
            } else {
                Aggregator.processByOrgUnit(indicatorsToProcess, outputFilePath);
            }
        } catch (FileNotFoundException ex) {
            log.error(ex);
        } catch (java.text.ParseException ex) {
            log.error(ex);
            log.error("Unable to parse date at row no: " + csvLineNumber);
        } catch (IOException ex) {
            log.error("Unable to read CSV file");
            log.error(ex);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                log.error(ex);
            }
        }
    }

}
