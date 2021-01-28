package com.healthit.mohdap.indicator_calculator.service;

import com.healthit.indicator_calculator.util.DatabaseSource;
import com.healthit.indicator_calculator.util.Stringzz;
import com.healthit.mohdap.indicator_calculator.AggregationType;
import com.healthit.mohdap.indicator_calculator.Entry;
import com.healthit.mohdap.indicator_calculator.Indicator;
import com.healthit.mohdap.indicator_calculator.IndicatorType;
import com.healthit.mohdap.indicator_calculator.OrgLevel;
import com.healthit.mohdap.indicator_calculator.OrgUnit;
import com.healthit.mohdap.indicator_calculator.Period;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import static org.hisp.dhis.antlr.AntlrParserUtils.castDouble;
import org.hisp.dhis.antlr.Parser;

/**
 *
 * @author duncanndiithi
 */
public class Aggregator {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(Aggregator.class.getCanonicalName());

    private static Cache<String, Double> aggregateValues;

    static {
        aggregateValues = Cache2kBuilder.of(String.class, Double.class).name("aggregate").eternal(true).entryCapacity(4000000).build();
    }

    private String aggregatorSql = "select @aggregation_type as val from datavalue dt \n"
            + "inner join period pe on  dt.periodid=pe.periodid\n"
            + "inner join organisationunit orgunit on dt.sourceid=orgunit.organisationunitid\n"
            + "inner join dataelement de on dt.dataelementid=de.dataelementid\n"
            + "inner join categoryoptioncombo comb on comb.categoryoptioncomboid  = dt.categoryoptioncomboid where ";
    //   + "where de.valuetype in ('NUMBER','INTEGER') ";

    private String aggregationTypeSql = "Select aggregationtype from dataelement where uid=?";

    /**
     *
     * @param elementId
     * @param comboId
     * @param period
     * @param orgunit
     * @return
     */
    public Double aggregateValuesDataElements(String elementId, String comboId, Period period, OrgUnit orgunit) {

        String cacheValueName = elementId + "" + comboId + "" + period.getId() + "" + orgunit.getId();
        Double value = aggregateValues.get(cacheValueName);

        if (value != null) {
            return value;
        }
        String conditionClause = null;
        if (comboId != null) {
            conditionClause = "(de.uid='" + elementId + "' and comb.uid ='" + comboId + "')";
        } else {
            conditionClause = "(de.uid='" + elementId + "')";
        }
        appendDataelementAggregationType(elementId);

        aggregatorSql = aggregatorSql + conditionClause + " and  pe.periodid=" + period.getId();

        OrgLevel orgunitLevel = orgunit.getHierarchylevel();
        appendOruntiSqlClause(orgunitLevel, orgunit.getId());
        aggregatorSql = aggregatorSql;

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        double calculatedValue = 0;
        try {
            conn = DatabaseSource.getConnection();
            log.info(aggregatorSql);
            ps = conn.prepareStatement(aggregatorSql);
            rs = ps.executeQuery();

            if (rs.next()) {
                calculatedValue = rs.getDouble(1);
            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        aggregateValues.put(cacheValueName, calculatedValue);
        return calculatedValue;
    }

    /**
     *
     * @param elementId
     */
    private void appendDataelementAggregationType(String elementId) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = DatabaseSource.getConnection();
            ps = conn.prepareStatement(aggregationTypeSql);
            ps.setString(1, elementId);
            rs = ps.executeQuery();

            if (rs.next()) {
                String aggregationType = rs.getString(1);
                if ("COUNT".contentEquals(aggregationType)) {
                    aggregatorSql = aggregatorSql.replaceAll("@aggregation_type as", "COUNT(CAST (dt.value AS double precision))");
                } else if ("NONE".contentEquals(aggregationType)) {
                    aggregatorSql = aggregatorSql.replaceAll("@aggregation_type as", "SUM(CAST (dt.value AS double precision))");
                } else if ("AVERAGE".contentEquals(aggregationType)) {
                    aggregatorSql = aggregatorSql.replaceAll("@aggregation_type as", "AVG(CAST (dt.value AS double precision))");
                } else if ("AVERAGE_SUM_ORG_UNIT".contentEquals(aggregationType)) {
                    aggregatorSql = aggregatorSql.replaceAll("@aggregation_type as", "AVG(CAST (dt.value AS double precision))");
                } else if ("SUM".contentEquals(aggregationType)) {
                    aggregatorSql = aggregatorSql.replaceAll("@aggregation_type as", "SUM(CAST (dt.value AS double precision))");
                } else {
                    aggregatorSql = aggregatorSql.replaceAll("@aggregation_type as", "SUM(CAST (dt.value AS double precision))");
                }
            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
    }

    private void appendOruntiSqlClause(OrgLevel orgunitLevel, int orgId) {
        String orgUnitIdsSql = getOrgunitsIdsToAggregate(orgunitLevel, orgId);

        aggregatorSql = aggregatorSql + " and orgunit.organisationunitid in(" + orgUnitIdsSql + ")";
    }

    /**
     *
     * @param orgunitLevel
     * @param orgId
     * @return
     */
    private String getOrgunitsIdsToAggregate(OrgLevel orgunitLevel, int orgId) {
        String getAllOrgids = "";

        switch (orgunitLevel) { // get all orunit ids for which we will aggregate the data elements based on the orgunit we are aggregating.
            case LEVEL_4:
                getAllOrgids = "select organisationunitid from organisationunit where parentid in('" + orgId + "')";
                break;
            case LEVEL_3:
                getAllOrgids = "select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "'))";
                break;
            case LEVEL_2:
                getAllOrgids = "select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "')))";
                break;
            case LEVEL_1:

                getAllOrgids = "select organisationunitid from organisationunit where parentid in(\n"
                        + "  select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid "
                        + "from organisationunit where parentid in('" + orgId + "')))\n"
                        + ")";
                break;
            default:
                getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "'";
                break;

        }
        return getAllOrgids;
    }

    private static Indicator extractIndicatorFromResultSet(ResultSet rs) throws SQLException {
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
        return indicator;
    }

    private static OrgUnit extractOrgunitFromResultSet(ResultSet rs) throws SQLException {

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

        return orgUnit;
    }

    private static Period extractPeriodFromResultSet(ResultSet rs) throws SQLException {

        Period period = new Period();
        period.setId(rs.getInt("periodid"));
        period.setStartDate(rs.getDate("startdate"));
        period.setEndDate(rs.getDate("enddate"));

        return period;
    }

    /**
     *
     * @param rs
     * @return
     */
    private static List<Indicator> formatIndicators(ResultSet rs) {

        List<Indicator> indicators = new ArrayList();
        try {

            while (rs.next()) {

                indicators.add(extractIndicatorFromResultSet(rs));
            }

        } catch (SQLException ex) {
            log.error(ex);
        }

        return indicators;

    }

    /**
     *
     * @param rs
     * @return
     */
    private static Map<String, Indicator> formatIndicatorsMap(ResultSet rs) {

        Map<String, Indicator> result = new HashMap();
        try {
            while (rs.next()) {
                String indicname = rs.getString("name");
                result.put("'"+indicname.trim()+"'", extractIndicatorFromResultSet(rs));

            }

        } catch (SQLException ex) {
            log.error(ex);
        }

        return result;

    }

    private static List<Indicator> getAllIndicators() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        List<Indicator> indicators = null;
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT indicatorid, indc.name,indic_tp.indicatorfactor as factor, numerator, denominator, indc.uid,indic_tp.name as type_name from \n"
                    + " indicator indc\n"
                    + " inner join indicatortype indic_tp on indc.indicatortypeid=indic_tp.indicatortypeid"; //where indicatorid=95445";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            indicators = formatIndicators(rs);
        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        return indicators;
    }

    private static Map<String, Indicator> getIndicatorsByName(List<String> indicatorsNames) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        Map<String, Indicator> indicators = null;
        log.debug("comma seperated string for prep stamnt "+Stringzz.buildCommaSeperatedString(indicatorsNames));
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT distinct indicatorid, indc.name,indic_tp.indicatorfactor as factor, numerator, denominator, indc.uid,indic_tp.name as type_name from \n"
                    + " indicator indc\n"
                    + " inner join indicatortype indic_tp on indc.indicatortypeid=indic_tp.indicatortypeid  where trim(indc.name) in (?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, Stringzz.buildCommaSeperatedString(indicatorsNames));
            log.debug(ps);
            rs = ps.executeQuery();

            indicators = formatIndicatorsMap(rs);

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        return indicators;
    }

    private static List<OrgUnit> getOrgUnits() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        List<OrgUnit> orgunits = new ArrayList();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT organisationunitid, \"name\", parentid, uid, hierarchylevel FROM public.organisationunit";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                if (rs.getInt("hierarchylevel") < 5) {//process facility orgunit and upwards only
                    OrgUnit orgUnit = extractOrgunitFromResultSet(rs);
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

    private static Map<String, Object> getOrgUnits(List<String> orgunitNames, boolean isByLevel) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        Map<String, Object> orgunits = new HashMap();
        Map<String, Object> parentAndOrgunits = new HashMap();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT organisationunitid, \"name\", parentid, uid, hierarchylevel FROM public.organisationunit where trim(name) in(?)";
            if (isByLevel) {
                sql = "SELECT org_child.organisationunitid, org_parent.name parent_name,org_child.name child_name, org_child.parentid, org_child.uid, org_child.hierarchylevel "
                        + "FROM organisationunit org_parent"
                        + "inner join organisationunit org_child on org_child.parentid=org_parent.organisationunitid where trim(org_parent.name) in(?)";
            }

            ps = conn.prepareStatement(sql);
            ps.setString(1, Stringzz.buildCommaSeperatedString(orgunitNames));
            
            rs = ps.executeQuery();

            while (rs.next()) {
                if (rs.getInt("hierarchylevel") < 5) {//process facility orgunit and upwards only
                    
                    OrgUnit orgUnit = extractOrgunitFromResultSet(rs);
                    if (isByLevel) {
                        String pName="'"+rs.getString("parent_name").trim()+"'";
                        if (parentAndOrgunits.containsKey(pName)) {
                            List<OrgUnit> orgsList = (List<OrgUnit>) parentAndOrgunits.get(pName);
                            orgsList.add(orgUnit);
                        } else {
                            parentAndOrgunits.put(pName, new ArrayList<OrgUnit>());
                            List<OrgUnit> orgsList = (List<OrgUnit>) parentAndOrgunits.get(pName);
                            orgsList.add(orgUnit);
                        }

                    } else {
                        orgunits.put("'"+rs.getString("name").trim()+"'", orgUnit);
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        if (isByLevel) {
            return parentAndOrgunits;
        }
        log.debug("Orgs to return "+orgunits.toString());
        return orgunits;

    }

    /**
     *
     * @param resultListing
     */
    private static void saveResultsToCsvFile(List<List> resultListing) {
        if (resultListing != null) {
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

    }

    private static List getCalculatedIndicator(Indicator indicator, OrgUnit orgUnit, Period period) {

        Object numeratorResult = Parser.visit(indicator.getNumerator().trim().replaceAll("\\s+", ""), new CommonExpressionVisitor(period, orgUnit));

        Object denomeratorResult = Parser.visit(indicator.getDenominator().trim().replaceAll("\\s+", ""), new CommonExpressionVisitor(period, orgUnit));

        if (numeratorResult == null || denomeratorResult == null) {
            return null;
        }

        log.info("period: ====>> " + period.getStartDate());

        List reslt = new ArrayList();
        reslt.add(indicator.getName());
        reslt.add(period.getStartDate());
        reslt.add(period.getEndDate());
        try {

            if (numeratorResult.toString().contains("Infinity")
                    || denomeratorResult.toString().contains("Infinity")
                    || numeratorResult.toString().contains("NaN")
                    || denomeratorResult.toString().contains("NaN")) {
                return null;
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
            return null;
        }

        return reslt;

    }

    public static void processAllIndicators() {
        System.out.println("Processing begins... ");

        List<Indicator> indicators = Aggregator.getAllIndicators();
        List<OrgUnit> orgunits = Aggregator.getOrgUnits();
        List<Period> periods = Aggregator.getPeriods();
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
                    List calculatedValues = getCalculatedIndicator(indicator, orgunit, period);
                    if (calculatedValues == null) {
                        continue;
                    } else {
                        resultListing.add(calculatedValues);
                    }

                }
            }
        }
        Aggregator.saveResultsToCsvFile(resultListing);

    }

    private static List<Period> getPeriods() {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        List<Period> periods = new ArrayList();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT periodid, startdate, enddate FROM period";// 5 -- monthly
            ps = conn.prepareStatement(sql);
            log.debug(ps);
            rs = ps.executeQuery();

            while (rs.next()) {
                Period period = extractPeriodFromResultSet(rs);
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

    private static Map<String, Period> getPeriods(List<String> pe) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        Map<String, Period> periods = new HashMap();
        try {
            conn = DatabaseSource.getConnection();
            String sql = "SELECT periodid, startdate, enddate FROM period  where to_char(\"startdate\", 'YYYY-dd-mm') in(?) "
                    + " and periodtypeid =5";// 5 -- monthly
            ps = conn.prepareStatement(sql);
            ps.setString(1, Stringzz.buildCommaSeperatedString(pe));
            log.debug(ps);
            rs = ps.executeQuery();

            while (rs.next()) {
                Period period = extractPeriodFromResultSet(rs);
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                String strDate = dateFormat.format(rs.getDate("startdate"));
                periods.put(strDate, period);
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

    private static Object[] wrapParametersToProcess(List<List<String>> indicatorsToProcess, boolean isByLevel) {
        List<String> indicators = new ArrayList();
        List<String> orgs = new ArrayList();
        List<String> periods = new ArrayList();

        for (List<String> lst : indicatorsToProcess) {
            indicators.add(lst.get(0));
            orgs.add(lst.get(1));
            periods.add(lst.get(2));

        }

        Object[] listToReturn = {getIndicatorsByName(indicators), getOrgUnits(orgs, isByLevel), getPeriods(periods)};
        return listToReturn;
    }

    public static void processByOrgUnit(List<List<String>> indicatorsToProcess, String outputFilePath) {
        log.debug(indicatorsToProcess.toString());
        Object[] listVals = wrapParametersToProcess(indicatorsToProcess, false);
        
        Map<String, Indicator> i = (Map<String, Indicator>) listVals[0];
        Map<String, Period> p = (Map<String, Period>) listVals[2];
        Map<String, OrgUnit> o = (Map<String, OrgUnit>) listVals[1];

        List<List> resultListing = new ArrayList();
        
        for (List<String> lst : indicatorsToProcess) {

            
            OrgUnit orgUnit = o.get("'"+lst.get(1).trim()+"'");
            Period period = p.get(lst.get(2));
            Indicator indicator = i.get("'"+lst.get(0).trim()+"'");
            
            List calculatedValues = getCalculatedIndicator(indicator, orgUnit, period);
            if (calculatedValues == null) {
                continue;
            } else {
                resultListing.add(calculatedValues);
            }

        }
        Aggregator.saveResultsToCsvFile(resultListing);

    }

    public static void processByOrgLevel(List<List<String>> indicatorsToProcess, String outputFilePath) {
        Object[] listVals = wrapParametersToProcess(indicatorsToProcess, false);

        Map<String, Indicator> i = (Map<String, Indicator>) listVals[0];
        Map<String, Period> p = (Map<String, Period>) listVals[2];
        Map<String, List<OrgUnit>> o = (Map<String, List<OrgUnit>>) listVals[1];

        List<List> resultListing = new ArrayList();
        for (List<String> lst : indicatorsToProcess) {
            List calculatedValues = null;
            for (Map.Entry<String, List<OrgUnit>> entry : o.entrySet()) {
                if (entry.getKey().equals(lst.get(1))) { //if map key which is parent org unit equals orgunit from user csv of the same indicator and period, then process

                    List<OrgUnit> childOrgs = entry.getValue();
                    for (OrgUnit childOrg : childOrgs) {
                        OrgUnit orgUnit = childOrg;
                        Period period = p.get(lst.get(2));
                        Indicator indicator = i.get(lst.get(0));
                        calculatedValues = getCalculatedIndicator(indicator, orgUnit, period);

                    }
                }

                if (calculatedValues == null) {
                    continue;
                } else {
                    resultListing.add(calculatedValues);
                }

            }
            Aggregator.saveResultsToCsvFile(resultListing);
        }

    }
}
