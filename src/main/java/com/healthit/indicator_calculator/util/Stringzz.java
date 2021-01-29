/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.healthit.indicator_calculator.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/**
 *
 * @author duncanndiithi
 */
public class Stringzz {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(Stringzz.class.getCanonicalName());

    private static final String processedItemsFile = "./processed_indicators.json";
    private static final String compressedProcessedItemsFile = "./processed_indicators.gzip";

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
        return readLastProcessedPoitJson(new ByteArrayOutputStream());
    }

    private static Map<String, Boolean> readLastProcessedPoitJson(ByteArrayOutputStream bos) {
        Map<String, Boolean> map = null;
        byte[] bytes = null;

        try {
            log.info("reading last process status");
            if (bos != null) {
                try {
                    bytes = Files.readAllBytes(Paths.get(compressedProcessedItemsFile));
                } catch (IOException ex) {
                    return null;
                }

            } else {
                bytes = bos.toByteArray();
            }

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                output.append(line);
            }
            Gson gson = new Gson();
            map = gson.fromJson(output.toString(), Map.class);

            return map;

        } catch (IOException ex) {
            log.error(ex);
            System.exit(0);
        }

        return map;
    }

    public static Map<String, Boolean> writeLastProcessedPointsJson(Map<String, Boolean> processesValuesMap) {
        ByteArrayOutputStream byteArrayOutputStream = null;
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        String outputJson = gson.toJson(processesValuesMap);

        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                gzipOutputStream.write(outputJson.getBytes(StandardCharsets.UTF_8));
            }
            byteArrayOutputStream.toByteArray();
            FileOutputStream fos = new FileOutputStream(compressedProcessedItemsFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            bos.write(byteArrayOutputStream.toByteArray());
            bos.flush();
            bos.close();
            fos.close();
        } catch (IOException e) {
            log.error(e);
        }
        return readLastProcessedPoitJson(byteArrayOutputStream);

    }
}
