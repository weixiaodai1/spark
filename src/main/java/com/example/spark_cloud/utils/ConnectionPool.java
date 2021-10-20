package com.example.spark_cloud.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;
public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    public ConnectionPool() {
    }

    public static synchronized Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList();

                for(int i = 0; i < 5; ++i) {
                    Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/vims", "root", "12345");
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception var2) {
            var2.printStackTrace();
        }

        return (Connection)connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException var1) {
            var1.printStackTrace();
        }

    }
}
