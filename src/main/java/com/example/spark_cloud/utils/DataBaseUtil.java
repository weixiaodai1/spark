package com.example.spark_cloud.utils;

public class DataBaseUtil {
    public static final String SPARK_MASTER = "local";
    public static final String SPARK_APPNAME = "test";
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_CONNECTION_ALL = "jdbc:mysql://localhost:3306/test?user=root&password=123456";
    public static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/test";
    public static final String MYSQL_CONNECTION_NAME = "root";
    public static final String MYSQL_CONNECTION_PASSWORD = "123456";
    public static final String MYSQL_TEST_SQL = "SELECT * FROM `one` ";
    public static final String CASSANDRA_HOSTNAME = "127.0.0.1";
    public static final String CASSANDRA_USERNAME = "hzweiyongqiang";
    public static final String CASSANDRA_PASSWORD = "postgres";
    public static final String ZOOKERPER_HOSTNAME = "zookerper.host1,zookerper.host2,zookerper.host3";
    public static final String HBASE_PRODUCT = "/hbase_product";
    public static final String ES_NODES = "elasticsearch.host";
    public static final String ES_RESOURCE_READ = "test_mailindex";
    public static final String ES_RESOURCE_WRITE = "test/mail";

    public DataBaseUtil() {
    }
}
