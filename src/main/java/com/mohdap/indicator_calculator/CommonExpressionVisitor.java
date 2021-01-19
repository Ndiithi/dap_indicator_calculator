package com.mohdap.indicator_calculator;

import com.healthit.indicator_calculator.util.DatabaseSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.hisp.dhis.antlr.AntlrExpressionVisitor;
import org.hisp.dhis.antlr.ParserExceptionWithoutContext;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static org.hisp.dhis.antlr.AntlrParserUtils.castDouble;
import org.hisp.dhis.parser.expression.antlr.ExpressionParser;

import static org.hisp.dhis.parser.expression.antlr.ExpressionParser.*;

/**
 * Common traversal of the ANTLR4 expression parse tree using the visitor
 * pattern.
 *
 * @author Jim Grace
 */
public class CommonExpressionVisitor extends AntlrExpressionVisitor {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(CommonExpressionVisitor.class.getCanonicalName());
    
    String catDataElementSubSql =" where de.uid='@elementid'";
    String catOptionComboSubSql = "inner join categoryoptioncombo comb on comb.categoryoptioncomboid  = dt.categoryoptioncomboid \n" +
                        "where de.uid='@elementid' and comb.uid ='@catoptcomboid'";
    
    protected CommonExpressionVisitor() {
    }

    @Override
    public Object visitExpr(ExprContext ctx) {

        if (ctx.it != null) {
            if (ctx.it.getType() == PAREN) { //parenthesis
                Object value = visit(ctx.expr(0));
                System.out.println("the value is: PAREN: " + value);
            }

            if (ctx.it.getType() == MINUS) { //minus

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = visit(ctx.expr(1)); // get value of right subexpression
                System.out.println("Minus " + left.toString() + " and " + right.toString());
//            if((String)left ==){
//                
//            }
//            
//            if ( ctx.op.getType() == LabeledExprParser.ADD ) return left + right;
//            return left - right; // must be SUB
            }

            if (ctx.it.getType() == MUL) { //MULtiplication

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = visit(ctx.expr(1)); // get value of right subexpression
                System.out.println("Multiplying " + left.toString() + " and " + right.toString());
                return ctx.getText();
            }

            if (ctx.it.getType() == HASH_BRACE) {
                System.out.println("resolving hash brace");
                System.out.println(ctx.uid0.getText());
                if (ctx.uid1 != null) {
                    System.out.println(ctx.uid1.getText());
                }
                Connection dbCon;
                try {
                    dbCon = DatabaseSource.getConnection();
                    String sql = "Select dt.value from datavalue dt \n"
                            + "inner join dataelement de on dt.dataelementid=de.dataelementid ";  
                    if(ctx.uid1!=null){
                    String appendSubSql=catOptionComboSubSql.replaceAll("@elementid", ctx.uid0.getText()).replaceAll("@catoptcomboid", ctx.uid1.getText());
                    sql=sql+appendSubSql;
                    }else{
                        String appendSubSql=catDataElementSubSql.replaceAll("@elementid", ctx.uid0.getText());
                        sql=sql+appendSubSql;
                    }
                    PreparedStatement st = dbCon.prepareStatement(sql);
                    ResultSet rs = st.executeQuery();
                    System.out.println("Got a number: testing");
                    while (rs.next()) {
                        System.out.println("Got a number: " + rs.getString(1));
                    }

                } catch (SQLException ex) {
                    log.error(ex);
                }
                return ctx.getText();

            } else if (ctx.numericLiteral() != null) {

                double x = BigDecimal.valueOf(castDouble(ctx.numericLiteral().getText()))
                        .doubleValue();
                return x;
            }
        }
        return 0;
    }

    @Override
    public Object visitProgramRuleVariablePart(ExpressionParser.ProgramRuleVariablePartContext ctx
    ) {
        System.out.println("visitProgramRuleVariablePart ");
        System.out.println(ctx.getText());
        return 0;
    }

    @Override
    public Object visitProgramRuleVariableName(ExpressionParser.ProgramRuleVariableNameContext ctx
    ) {
        System.out.println("visitProgramRuleVariableName ");
        System.out.println(ctx.getText());
        return 0;
    }

    @Override
    public Object visitStringLiteral(StringLiteralContext ctx
    ) {
        System.out.println("visitStringLiteral ");
        System.out.println(ctx.getText());
        return 0;
    }

    @Override
    public Object visitNumericLiteral(NumericLiteralContext ctx
    ) {
        System.out.println("visitNumericLiteral ");
        System.out.println(ctx.getText());
        return 0;
    }

}
