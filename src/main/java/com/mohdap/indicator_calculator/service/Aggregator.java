/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mohdap.indicator_calculator.service;

import com.healthit.indicator_calculator.util.DatabaseSource;
import com.mohdap.indicator_calculator.OrgLevel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author duncanndiithi
 */
public class Aggregator {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(Aggregator.class.getCanonicalName());

    String aggregatorSql = "select SUM(CAST(dt.value AS integer)) as val from datavalue dt \n"
            + "inner join period pe on  dt.periodid=pe.periodid\n"
            + "inner join organisationunit orgunit on dt.sourceid=orgunit.organisationunitid\n"
            + "inner join dataelement de on dt.dataelementid=de.dataelementid\n"
            + "inner join categoryoptioncombo comb on comb.categoryoptioncomboid  = dt.categoryoptioncomboid \n"
            + "where  ";

    private void aggregateValuesByOrgUnit(String elementId, String comboId, int orgId) {
        String conditionClause = null;
        if (comboId != null) {
            conditionClause = "(de.uid='" + elementId + "' and comb.uid ='" + comboId + "')";
        } else {
            conditionClause = "(de.uid='" + elementId + "')";
        }
        aggregatorSql = aggregatorSql + conditionClause;

        OrgLevel orgunitLevel = getOrgUnitLevel(orgId);
        appendOruntiSqlClause(orgunitLevel, orgId);

    }

    private void appendOruntiSqlClause(OrgLevel orgunitLevel, int orgId) {
        List<Integer> orgUnitIds = getOrgunitsIdsToAggregate(orgunitLevel, orgId);

    }

    private List<Integer> getOrgunitsIdsToAggregate(OrgLevel orgunitLevel, int orgId) {
        String getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "'";

        switch (orgunitLevel) { // get all orunit ids for which we will aggregate the data elements based on the orgunit we are aggregating.
            case LEVEL_4:
                getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "' \n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in('" + orgId + "')";
                break;
            case LEVEL_3:
                getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "' \n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in('" + orgId + "')\n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "'))";
                break;
            case LEVEL_2:
                getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "' \n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in('" + orgId + "')\n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "'))\n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "')))";
            case LEVEL_1:
                getAllOrgids = "select organisationunitid from organisationunit where organisationunitid='" + orgId + "' \n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in('" + orgId + "')\n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "'))\n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "')))\n"
                        + "union\n"
                        + "select organisationunitid from organisationunit where parentid in(\n"
                        + "  select organisationunitid from organisationunit where parentid in\n"
                        + "  (select organisationunitid from organisationunit where parentid in(select organisationunitid from organisationunit where parentid in('" + orgId + "')))\n"
                        + ")";
        }

        Connection dbCon;
        List<Integer> orgIds = new ArrayList<Integer>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            dbCon = DatabaseSource.getConnection();
            ps = dbCon.prepareStatement(getAllOrgids);
            rs = ps.executeQuery();

            while (rs.next()) {
                orgIds.add(rs.getInt(1));
            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }
        return orgIds;
    }

    private OrgLevel getOrgUnitLevel(int orgId) {

        int orgLevel = 1; //Kenya
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = DatabaseSource.getConnection();
            String sql = "Select hierarchylevel from organisationunit where organisationunitid= " + orgId;
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            if (rs.next()) {
                orgLevel = rs.getInt(1);
            }

        } catch (SQLException ex) {
            log.error(ex);
        } finally {
            DatabaseSource.close(rs);
            DatabaseSource.close(ps);
            DatabaseSource.close(conn);
        }

        switch (orgLevel) {
            case 1:
                return OrgLevel.LEVEL_1;
            case 2:
                return OrgLevel.LEVEL_2;
            case 3:
                return OrgLevel.LEVEL_3;
            case 4:
                return OrgLevel.LEVEL_4;
            case 5:
                return OrgLevel.LEVEL_5;
            default:
                return OrgLevel.LEVEL_1;
        }

    }

}
