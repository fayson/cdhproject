package com.cloudera.hivejdbc;

import com.cloudera.utils.JDBCUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * package: com.cloudera.hivejdbc
 * describe: 集群中只启用了Sentry服务如何访问Hive
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/1/15
 * creat_time: 下午8:57
 * 公众号：Hadoop实操
 */
public class SentryOnlyDemo {

    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://ip-172-31-6-148.fayson.com:10000/";
    private static String username = "hive";
    private static String password = "";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("集群中只启用了Sentry服务访问Hive");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL, username, password);
            ps = connection.prepareStatement("show databases");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.disconnect(connection, rs, ps);
        }
    }
}
