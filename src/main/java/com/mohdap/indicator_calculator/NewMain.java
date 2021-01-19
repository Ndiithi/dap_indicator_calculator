/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mohdap.indicator_calculator;

import com.healthit.indicator_calculator.util.DatabaseSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hisp.dhis.antlr.Parser;

/**
 *
 * @author duncanndiithi
 */
public class NewMain {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(NewMain.class.getCanonicalName());

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        System.out.println("value to parse is: ");
        Object result = Parser.visit("#{PfGYqI66BCy}*4-#{PfGYqI66BCy.B1sPPfIobef}", new CommonExpressionVisitor());
        System.out.println("");

    }

}
