/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.healthit.indicator_calculator.util;

import java.util.List;

/**
 *
 * @author duncanndiithi
 */
public class Stringzz {
    public static String buildCommaSeperatedString(List<String> values){
        StringBuilder commaSeperatedValues = new StringBuilder();
        boolean added = false;
        for (String value : values) {
            if (added) {
                commaSeperatedValues.append("," + "'" + value + "'");
            } else {
                commaSeperatedValues.append("'"+value+"'");
                added = true;
            }
        }
        return commaSeperatedValues.toString();
    }
}
