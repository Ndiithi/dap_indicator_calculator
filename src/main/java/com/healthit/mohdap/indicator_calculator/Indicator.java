package com.healthit.mohdap.indicator_calculator;

/**
 *
 * @author duncanndiithi
 */
public class Indicator {

    private String name;
    private String uuid;
    private int id;
    private int factor;
    private String numerator;
    private String denominator;
    private IndicatorType indicatorType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public IndicatorType getIndicatorType() {
        return indicatorType;
    }

    public void setIndicatorType(IndicatorType indicatorType) {
        this.indicatorType = indicatorType;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getFactor() {
        return factor;
    }

    public void setFactor(int factor) {
        this.factor = factor;
    }

    public String getNumerator() {
        return numerator;
    }

    public void setNumerator(String numerator) {
        this.numerator = numerator;
    }

    public String getDenominator() {
        return denominator;
    }

    public void setDenominator(String denominator) {
        this.denominator = denominator;
    }

}
