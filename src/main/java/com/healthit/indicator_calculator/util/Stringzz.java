/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.healthit.indicator_calculator.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.nashorn.internal.parser.JSONParser;

/**
 *
 * @author duncanndiithi
 */
public class Stringzz {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(Stringzz.class.getCanonicalName());

    private static final String processedItemsFile = "./processed_indicators.json";

    public static String buildCommaSeperatedString(List<String> values) {
        StringBuilder commaSeperatedValues = new StringBuilder();
        boolean added = false;
        for (String value : values) {
            if (added) {
                commaSeperatedValues.append("," + "'" + value + "'");
            } else {
                commaSeperatedValues.append("'" + value + "'");
                added = true;
            }
        }
        return commaSeperatedValues.toString();
    }

    public static Map<String, Boolean> readLastProcessedPoitJson() {
        Map<String, Boolean> map = null;
        log.info("reading last process status");
        try {
            Gson gson = new Gson();
            File file = new File(processedItemsFile);
            if (file.createNewFile()) {
                log.info("File has been created.");
            } else {
                log.info("File already exists.");
            }
            Reader reader = Files.newBufferedReader(Paths.get(processedItemsFile));
            map = gson.fromJson(reader, Map.class);

        } catch (FileNotFoundException ex) {
            log.error(ex);
            System.exit(0);
        } catch (IOException ex) {
            log.error(ex);

        }
        return map;
    }

    public static Map<String, Boolean> writeLastProcessedPoitJson(Map<String, Boolean> processesValuesMap) {

        FileWriter writer = null;
        try {
            GsonBuilder builder = new GsonBuilder();
            Gson gson = builder.create();
            writer = new FileWriter(processedItemsFile);
            writer.write(gson.toJson(processesValuesMap));
            writer.close();
            return readLastProcessedPoitJson();
        } catch (IOException ex) {
            log.error(ex);
        } finally {
            try {
                writer.close();
            } catch (IOException ex) {
                log.error(ex);
            }
        }
        return readLastProcessedPoitJson();
    }
}
