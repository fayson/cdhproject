package com.cloudera.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * package: com.cloudera.utils
 * describe: JDBC工具类，包含关闭连接，获取数据等等。
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/11/21
 * creat_time: 下午10:28
 * 公众号：Hadoop实操
 */
public class JDBCUtils {

    /**
     * 关闭数据库连接
     * @param connection
     * @param res
     * @param ps
     * @throws SQLException
     */
    public static void disconnect(Connection connection, ResultSet res, PreparedStatement ps) {

        try {
            if (res != null)  res.close();
            if (ps != null) ps.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
