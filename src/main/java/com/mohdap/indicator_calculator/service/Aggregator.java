package com.mohdap.indicator_calculator.service;

import com.healthit.indicator_calculator.util.DatabaseSource;
import com.mohdap.indicator_calculator.AggregationType;
import com.mohdap.indicator_calculator.OrgLevel;
import com.mohdap.indicator_calculator.OrgUnit;
import com.mohdap.indicator_calculator.Period;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

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

        aggregatorSql = aggregatorSql  + conditionClause + " and  pe.periodid=" + period.getId();

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

}
