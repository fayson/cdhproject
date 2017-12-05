package com.cloudera.hivejdbc;

import com.cloudera.utils.JDBCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * package: com.cloudera.hivejdbc
 * describe: 该事例主要讲述通过JDBC连接Kerberos环境下的Hive
 * creat_user: Fayson
 * email: htechinfo@163.com
 * 公众号：Hadoop实操
 * creat_date: 2017/11/21
 * creat_time: 下午9:03
 */
public class KBSimple {
    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://54.179.148.25:10000/;principal=hive/ip-172-31-22-86.ap-southeast-1.compute.internal@CLOUDERA.COM";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("通过JDBC连接Kerberos环境下的HiveServer2");
        //登录Kerberos账号
        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication" , "Kerberos" );
        UserGroupInformation. setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("fayson@CLOUDERA.COM", "/Volumes/Transcend/keytab/fayson.keytab");
        System.out.println(UserGroupInformation.getLoginUser());

        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps = connection.prepareStatement("select * from test_table");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1) + "----" + rs.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.disconnect(connection, rs, ps);
        }
    }
}
