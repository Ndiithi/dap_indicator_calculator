/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.healthit.indicator_calculator.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.log4j.Logger;
import java.util.Properties;
import java.util.Set;

/**
 *
 * @author duncan
 */
public class PropertiesLoader {

    final static Logger log = Logger.getLogger(PropertiesLoader.class.getCanonicalName());

    public static Properties getPropertiesFile( String propertyFileName) {
        Properties propertyFile=null;
        log.info("loading properties file: "+ propertyFileName);
        try {
            if (propertyFile == null) {
                log.debug("properties file object null, loading to memory");
                FileInputStream file = new FileInputStream(propertyFileName);

                propertyFile = new Properties();
                //InputStream s = Properties.class.getResourceAsStream("query_matcher.properties");
                propertyFile.load(file);
                file.close();
            }

        } catch (IOException ex) {
            log.error(ex);
        }
        return propertyFile;
    }

    public Set<Object> getAllKeys(Properties prop) {
        Set<Object> keys = prop.keySet();
        return keys;
    }

}
