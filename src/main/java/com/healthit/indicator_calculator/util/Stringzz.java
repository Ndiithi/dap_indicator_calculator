/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.healthit.indicator_calculator.util;

import com.google.gson.Gson;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
        String processedItemsFile = "./processed_indicators.json";
        Map<String, Boolean> map = null;
        try {
            Gson gson = new Gson();
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
}
