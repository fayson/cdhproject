package com.cloudera.impalajdbc;

import com.cloudera.utils.JDBCUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * package: com.cloudera.impala
 * describe: 该事例主要讲述通过JDBC连接非Kerberos环境下的Impala
 * creat_user: Fayson
 * email: htechinfo@163.com
 * 公众号：Hadoop实操
 * creat_date: 2017/11/21
 * creat_time: 下午7:32
 */
public class NoneKBSimple {

    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL ="jdbc:impala://54.255.237.128:21050/";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("通过JDBC连接非Kerberos环境下的Impala");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps = connection.prepareStatement("select * from test_table");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1) + "-------" + rs.getString(2));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.disconnect(connection, rs, ps);
        }
    }
}
