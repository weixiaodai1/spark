package com.example.spark_cloud.utils;

public class Cal_sta {
    public Cal_sta() {
    }

    public double Sum(Double[] data) {
        double sum = 0.0D;

        for(int i = 0; i < data.length; ++i) {
            sum += data[i];
        }

        return sum;
    }

    public double Mean(Double[] data) {
        double mean = 0.0D;
        mean = this.Sum(data) / (double)data.length;
        return mean;
    }

    public double POP_Variance(Double[] data) {
        double variance = 0.0D;

        for(int i = 0; i < data.length; ++i) {
            variance += Math.pow(data[i] - this.Mean(data), 2.0D);
        }

        variance /= (double)data.length;
        return variance;
    }

    public double POP_STD_dev(Double[] data) {
        double std_dev = Math.sqrt(this.POP_Variance(data));
        return std_dev;
    }

    public double Sample_Variance(Double[] data) {
        double variance = 0.0D;

        for(int i = 0; i < data.length; ++i) {
            variance += Math.pow(data[i] - this.Mean(data), 2.0D);
        }

        variance /= (double)(data.length - 1);
        return variance;
    }

    public double Sample_STD_dev(Double[] data) {
        double std_dev = Math.sqrt(this.Sample_Variance(data));
        return std_dev;
    }
}
