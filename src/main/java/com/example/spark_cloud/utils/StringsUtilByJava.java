package com.example.spark_cloud.utils;

public class StringsUtilByJava {
    public StringsUtilByJava() {
    }

    public static String getMethodName() {
        return (new Exception()).getStackTrace()[1].getMethodName();
    }

    public static void printFinish() {
        System.out.println("----------  " + (new Exception()).getStackTrace()[1].getMethodName() + "  ----------");
    }

    public static void main(String[] args) {
        System.out.println(getMethodName());
    }
}
