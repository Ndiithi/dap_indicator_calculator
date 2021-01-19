/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mohdap.indicator_calculator.service;

import com.healthit.indicator_calculator.util.DatabaseSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author duncanndiithi
 */
public class Aggregator {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(Aggregator.class.getCanonicalName());

    private int getOrgUnitLevel(int orgId) {

        Connection dbCon;
        int orgLevel = 1; //Kenya
        try {
            dbCon = DatabaseSource.getConnection();
            String sql = "Select hierarchylevel from organisationunit where organisationunitid= "+orgId;
            PreparedStatement st = dbCon.prepareStatement(sql);
            ResultSet rs = st.executeQuery();
            
            if (rs.next()) {
                orgLevel= rs.getInt(1);
            }

        } catch (SQLException ex) {
            log.error(ex);
        }
        return orgLevel;
    }
    
   
}
