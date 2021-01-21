package com.mohdap.indicator_calculator.service;

import com.healthit.indicator_calculator.util.DatabaseSource;
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

    String aggregatorSql = "select SUM(CAST (dt.value AS double precision)) as val from datavalue dt \n"
            + "inner join period pe on  dt.periodid=pe.periodid\n"
            + "inner join organisationunit orgunit on dt.sourceid=orgunit.organisationunitid\n"
            + "inner join dataelement de on dt.dataelementid=de.dataelementid\n"
            + "inner join categoryoptioncombo comb on comb.categoryoptioncomboid  = dt.categoryoptioncomboid \n"
            + "where de.aggregationtype='SUM' and de.valuetype in ('NUMBER','INTEGER') ";

    public double aggregateValuesDataElements(String elementId, String comboId, Period period, OrgUnit orgunit) {

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
        aggregatorSql = aggregatorSql + " and " + conditionClause + " and  pe.periodid=" + period.getId();

        OrgLevel orgunitLevel = orgunit.getHierarchylevel();
        appendOruntiSqlClause(orgunitLevel, orgunit.getId());
        aggregatorSql = aggregatorSql;

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        double calculatedValue = 0;
        System.out.println(aggregatorSql);
        try {
            conn = DatabaseSource.getConnection();

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

    private void appendOruntiSqlClause(OrgLevel orgunitLevel, int orgId) {
        String orgUnitIdsSql = getOrgunitsIdsToAggregate(orgunitLevel, orgId);

        aggregatorSql = aggregatorSql + " and orgunit.organisationunitid in(" + orgUnitIdsSql + ")";
    }

    private String getOrgunitsIdsToAggregate(OrgLevel orgunitLevel, int orgId) {
        String getAllOrgids = "";

        switch (orgunitLevel) { // get all orunit ids for which we will aggregate the data elements based on the orgunit we are aggregating.
            case LEVEL_4:
                log.debug("org level: ============> 4");
                getAllOrgids = "select organisationunitid from organisationunit where parentid in('" + orgId + "')";
                //  log.info(getAllOrgids);
                break;
            case LEVEL_3:
                log.debug("org level: ============> 3");
                getAllOrgids = "select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "'))";
                //  log.info(getAllOrgids);
                break;
            case LEVEL_2:
                log.debug("org level: ============> 2");
                getAllOrgids = "select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "')))";
                //log.info(getAllOrgids);
                break;
            case LEVEL_1:
                log.debug("org level: ============> 1");

                getAllOrgids = "select organisationunitid from organisationunit where parentid in(\n"
                        + "  select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid "
                        + "from organisationunit where parentid in('" + orgId + "')))\n"
                        + ")";
                // log.info(getAllOrgids);
                break;
            default:
                getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "'";
                break;

        }
        return getAllOrgids;
    }

}
