/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mohdap.indicator_calculator;

import com.healthit.indicator_calculator.util.DatabaseSource;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import static org.hisp.dhis.antlr.AntlrParserUtils.castDouble;
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
        System.out.println("Processing begins... ");

        List<Indicator> indicators = NewMain.getAllIndicators();
        List<OrgUnit> orgunits = NewMain.getAllOrgUnits();
        List<Period> periods = NewMain.getAllPeriods();
        List<List> resultListing = new ArrayList();
        for (Indicator indicator : indicators) {
            log.info("indicator name ===> " + indicator.getName());
            log.info("numerator to evaluate ===> " + indicator.getNumerator());
            log.info("Denominator to evaluate ===> " + indicator.getDenominator());
            if (indicator.getNumerator() == null || indicator.getDenominator() == null) {
                continue;
            }
            if (indicator.getNumerator().length() == 0 || indicator.getDenominator().length() == 0) {
                continue;
            }

            for (OrgUnit orgunit : orgunits) {
                for (Period period : periods) {
                    Object numeratorResult = Parser.visit(indicator.getNumerator().trim().replaceAll("\\s+", ""), new CommonExpressionVisitor(period, orgunit));

                    Object denomeratorResult = Parser.visit(indicator.getDenominator().trim().replaceAll("\\s+", ""), new CommonExpressionVisitor(period, orgunit));

                    if (numeratorResult == null || denomeratorResult == null) {
                        continue;
                    }

                    log.info("period: ====>> " + period.getStartDate());

                    List reslt = new ArrayList();
                    reslt.add(indicator.getName());
                    reslt.add(period.getStartDate());
                    reslt.add(period.getEndDate());
                    try {
                        log.info("numerator value: ====>> " + numeratorResult.toString());
                        log.info("denomenator value: ====>> " + denomeratorResult.toString());
                        if (numeratorResult.toString().contains("Infinity")
                                || denomeratorResult.toString().contains("Infinity")
                                || numeratorResult.toString().contains("NaN")
                                || denomeratorResult.toString().contains("NaN")) {
                            continue;
                        }
                        Double num = BigDecimal.valueOf(castDouble(numeratorResult.toString()))
                                .doubleValue();
                        Double denom = BigDecimal.valueOf(castDouble(denomeratorResult.toString()))
                                .doubleValue();

                        log.info("denomenator value: ====>> " + BigDecimal.valueOf(castDouble(denomeratorResult.toString())));
                        log.info("factor value: ====>> " + indicator.getFactor());
                        Double results = (num / denom) * indicator.getFactor();

                        log.info("results value: ====>> " + results);
                        if (indicator.getIndicatorType() == IndicatorType.NUMBER) {

                            if (results == Double.POSITIVE_INFINITY || results == Double.NEGATIVE_INFINITY) {
                                reslt.add(0.0);
                            } else {
                                reslt.add(results.intValue());
                            }

                        } else {
                            reslt.add(results);
                        }

                    } catch (ArithmeticException ex) {
                        continue;
                    }

                    resultListing.add(reslt);
                }
            }
        }
        NewMain.saveResultsToCsvFile(resultListing);

    }

    private static void saveResultsToCsvFile(List<List> resultListing) {

        FileWriter out = null;
        String[] HEADERS = {"Indicator", "Start_date", "End_date", "Value"};
        try {
            out = new FileWriter("calculated_indicators.csv");

            try (CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
                    .withHeader(HEADERS))) {
                for (List rslt : resultListing) {
                    printer.printRecord(rslt.get(0), rslt.get(1), rslt.get(2), rslt.get(3));
                }

            }

        } catch (IOException ex) {
            log.error(ex);
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                log.error(ex);
            }
        }
    }

    private static List<Period> getAllPeriods() {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        List<Period> periods = new ArrayList();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT periodid, startdate, enddate FROM period";//  where startdate >'2018-12-31'"
            //   + " and startdate <'2019-02-01' and periodtypeid =5";// 5 -- monthly
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Period period = new Period();
                period.setId(rs.getInt("periodid"));
                period.setStartDate(rs.getDate("startdate"));
                period.setEndDate(rs.getDate("enddate"));
                periods.add(period);
            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        return periods;

    }

    private static List<Indicator> getAllIndicators() {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        List<Indicator> indicators = new ArrayList();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT indicatorid, indc.name,indic_tp.indicatorfactor as factor, numerator, denominator, indc.uid,indic_tp.name as type_name from \n"
                    + " indicator indc\n"
                    + " inner join indicatortype indic_tp on indc.indicatortypeid=indic_tp.indicatortypeid";// where indicatorid=32966";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Indicator indicator = new Indicator();
                indicator.setId(rs.getInt("indicatorid"));
                indicator.setName(rs.getString("name"));
                indicator.setFactor(rs.getInt("factor"));
                indicator.setUuid(rs.getString("uid"));
                indicator.setNumerator(rs.getString("numerator"));
                indicator.setDenominator(rs.getString("denominator"));
                String typeName = rs.getString("type_name");
                if (typeName.equals("Percentage")) {
                    indicator.setIndicatorType(IndicatorType.PERCENTAGE);
                } else if (typeName.equals("Per 1000")) {
                    indicator.setIndicatorType(IndicatorType.PER_1000);
                } else if (typeName.equals("Per 100 000")) {
                    indicator.setIndicatorType(IndicatorType.PER_100000);
                } else if (typeName.equals("Rate (factor=1)")) {
                    indicator.setIndicatorType(IndicatorType.RATE_1);
                } else if (typeName.equals("Per 10 000")) {
                    indicator.setIndicatorType(IndicatorType.PER_10000);
                } else if (typeName.equals("Per 5 000")) {
                    indicator.setIndicatorType(IndicatorType.PER_5000);
                } else if (typeName.equals("Number")) {
                    indicator.setIndicatorType(IndicatorType.NUMBER);
                }

                indicators.add(indicator);
            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        return indicators;

    }

    private static List<OrgUnit> getAllOrgUnits() {

        int orgLevel = 1; //Kenya
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        List<OrgUnit> orgunits = new ArrayList();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT organisationunitid, \"name\", parentid, uid, hierarchylevel FROM public.organisationunit where organisationunitid=18";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                if (rs.getInt("hierarchylevel") < 5) {//process facility orgunit and upwards only
                    OrgUnit orgUnit = new OrgUnit();
                    orgUnit.setId(rs.getInt("organisationunitid"));
                    orgUnit.setName(rs.getString("name"));
                    orgUnit.setParentId(rs.getInt("parentid"));
                    orgUnit.setUuid(rs.getString("uid"));

                    switch (rs.getInt("hierarchylevel")) {
                        case 1:
                            orgUnit.setHierarchylevel(OrgLevel.LEVEL_1);
                            break;
                        case 2:
                            orgUnit.setHierarchylevel(OrgLevel.LEVEL_2);
                            break;
                        case 3:
                            orgUnit.setHierarchylevel(OrgLevel.LEVEL_3);
                            break;
                        case 4:
                            orgUnit.setHierarchylevel(OrgLevel.LEVEL_4);
                            break;
                        case 5:
                            orgUnit.setHierarchylevel(OrgLevel.LEVEL_5);
                            break;
                        default:
                            orgUnit.setHierarchylevel(OrgLevel.LEVEL_1);
                            break;
                    }
                    orgunits.add(orgUnit);
                }

            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        return orgunits;

    }

}
