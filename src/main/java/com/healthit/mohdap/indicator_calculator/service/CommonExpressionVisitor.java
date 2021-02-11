package com.healthit.mohdap.indicator_calculator.service;

import com.healthit.mohdap.indicator_calculator.OrgUnit;
import com.healthit.mohdap.indicator_calculator.Period;
import com.healthit.mohdap.indicator_calculator.service.Aggregator;
import java.math.BigDecimal;
import org.hisp.dhis.antlr.AntlrExpressionVisitor;
import static org.hisp.dhis.antlr.AntlrParserUtils.castDouble;
import org.hisp.dhis.parser.expression.antlr.ExpressionParser;

import static org.hisp.dhis.parser.expression.antlr.ExpressionParser.*;

/**
 * Common traversal of the ANTLR4 expression parse tree using the visitor
 * pattern.
 *
 * @author Duncan Ndiithi, @Attribute Jim Grace
 */
public class CommonExpressionVisitor extends AntlrExpressionVisitor {

    final static org.apache.log4j.Logger log
            = org.apache.log4j.Logger.getLogger(CommonExpressionVisitor.class.getCanonicalName());

    private Period period;
    private OrgUnit orgunit;

    protected CommonExpressionVisitor(Period period, OrgUnit orgunit) {
        this.period = period;
        this.orgunit = orgunit;
    }

    @Override
    public Object visitExpr(ExprContext ctx) {

        if (ctx.it != null) {
            log.info("Resolve expression");
            if (ctx.it.getType() == PAREN) { //parenthesis
                Object value = visit(ctx.expr(0));

                return Double.parseDouble(value.toString());
            }

            if (ctx.it.getType() == MINUS) { //minus

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = 0;
                if (ctx.expr().size() > 1) {
                    right = visit(ctx.expr(1)); // get value of right subexpression
                }
                System.out.println("Minus " + left.toString() + " and " + right.toString());

                return Double.parseDouble(left.toString()) - Double.parseDouble(right.toString());

            }

            if (ctx.it.getType() == PLUS) { //plus
                System.out.println("Plus " + ctx.getText());
                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = 0;
                if (ctx.expr().size() > 1) {
                    right = visit(ctx.expr(1)); // get value of right subexpression
                }                //System.out.println("Plus " + left.toString() + " and " + right.toString());

                return Double.parseDouble(left.toString()) + Double.parseDouble(right.toString());

            }

            if (ctx.it.getType() == POWER) { //power

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = 0;
                if (ctx.expr().size() > 1) {
                    right = visit(ctx.expr(1)); // get value of right subexpression
                }

                System.out.println("Power " + left.toString() + " and " + right.toString());

                return Math.pow(Double.parseDouble(left.toString()), Double.parseDouble(right.toString()));

            }

            if (ctx.it.getType() == DIV) { //division

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = 0;
                if (ctx.expr().size() > 1) {
                    right = visit(ctx.expr(1)); // get value of right subexpression
                }

                return Double.parseDouble(left.toString()) / Double.parseDouble(right.toString());
            }

            if (ctx.it.getType() == MOD) { //modulus

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = 0;
                if (ctx.expr().size() > 1) {
                    right = visit(ctx.expr(1)); // get value of right subexpression
                }
                System.out.println("modulus " + left.toString() + " and " + right.toString());

                return Double.parseDouble(left.toString()) % Double.parseDouble(right.toString());

            }

            if (ctx.it.getType() == MUL) { //MULtiplication

                Object left = visit(ctx.expr(0));  // get value of left subexpression
                Object right = 0;
                if (ctx.expr().size() > 1) {
                    right = visit(ctx.expr(1)); // get value of right subexpression
                }
                System.out.println("MULtiplication " + left.toString() + " and " + right.toString());

                return Double.parseDouble(left.toString()) * Double.parseDouble(right.toString());

            }

            if (ctx.it.getType() == NUMERIC_LITERAL) {
                System.out.println("Numeric literal");
                System.out.println(ctx.getText());

                return ctx.getText();

            }

            if (ctx.it.getType() == HASH_BRACE) {
                System.out.println("resolving hash brace");
                System.out.println(ctx.uid0.getText());

                String comboId = null;
                if (ctx.uid1 != null) {
                    comboId = ctx.uid1.getText();
                }

                Aggregator agg = new Aggregator();
                Double aggregatedValue = agg.aggregateValuesDataElements(ctx.uid0.getText(), comboId, period, orgunit);

                return aggregatedValue;

            } else if (ctx.numericLiteral() != null) {

                Double val = BigDecimal.valueOf(castDouble(ctx.numericLiteral().getText()))
                        .doubleValue();
                return val;
            }

        } else {
            if (ctx.numericLiteral() != null) {
                Double val = BigDecimal.valueOf(castDouble(ctx.numericLiteral().getText()))
                        .doubleValue();
                return val;
            };
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
