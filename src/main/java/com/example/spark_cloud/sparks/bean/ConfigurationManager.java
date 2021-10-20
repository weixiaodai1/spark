package com.example.spark_cloud.sparks.bean;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    private static Properties prop = new Properties();

    public ConfigurationManager() {
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key) {
        String value = getProperty(key);

        try {
            return Integer.valueOf(value);
        } catch (Exception var3) {
            var3.printStackTrace();
            return 0;
        }
    }

    public static Boolean getBoolean(String key) {
        String value = getProperty(key);

        try {
            return Boolean.valueOf(value);
        } catch (Exception var3) {
            var3.printStackTrace();
            return false;
        }
    }

    public static Long getLong(String key) {
        String value = getProperty(key);

        try {
            return Long.valueOf(value);
        } catch (Exception var3) {
            var3.printStackTrace();
            return 0L;
        }
    }

    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("kafka.properties");
            prop.load(in);
        } catch (Exception var1) {
            var1.printStackTrace();
        }

    }
}
