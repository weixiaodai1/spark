package com.example.spark_cloud.sparks.bean;

public class Lines {
    String linesName;
    String marks;
    String scoreAverage;
    String priceVariance;

    public Lines() {
    }

    public String getLinesName() {
        return this.linesName;
    }

    public void setLinesName(String linesName) {
        this.linesName = linesName;
    }

    public String getMarks() {
        return this.marks;
    }

    public void setMarks(String marks) {
        this.marks = marks;
    }

    public String getScoreAverage() {
        return this.scoreAverage;
    }

    public void setScoreAverage(String scoreAverage) {
        this.scoreAverage = scoreAverage;
    }

    public String getPriceVariance() {
        return this.priceVariance;
    }

    public void setPriceVariance(String priceVariance) {
        this.priceVariance = priceVariance;
    }
}
