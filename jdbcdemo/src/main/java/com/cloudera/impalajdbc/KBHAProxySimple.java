package com.cloudera.impalajdbc;

import com.cloudera.utils.JDBCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * package: com.cloudera.impalajdbc
 * describe: 该事例主要讲述通过JDBC连接HAProxy访问Kerberos环境下的Impala
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/22
 * creat_time: 下午11:48
 * 公众号：Hadoop实操
 *
 * 需要注意的是KrbHostFQDN的地址与HAProxy地址一致，否则会报认证失败的异常。
 */
public class KBHAProxySimple {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://ip-172-31-22-86.ap-southeast-1.compute.internal:25004/default;AuthMech=1;KrbRealm=CLOUDERA.COM;KrbHostFQDN=ip-172-31-22-86.ap-southeast-1.compute.internal;KrbServiceName=impala";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("通过JDBC连接HAProxy访问Kerberos环境下的Impala");
        //登录Kerberos账号
        try {
            System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication" , "Kerberos");
            UserGroupInformation. setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab("fayson@CLOUDERA.COM", "/Volumes/Transcend/keytab/fayson.keytab");
            System.out.println(UserGroupInformation.getCurrentUser() + "------" + UserGroupInformation.getLoginUser());

            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

            loginUser.doAs(new PrivilegedAction<Object>(){

                public Object run() {
                    Connection connection = null;
                    ResultSet rs = null;
                    PreparedStatement ps = null;
                    try {
                        Class.forName(JDBC_DRIVER);
                        connection = DriverManager.getConnection(CONNECTION_URL);
                        ps = connection.prepareStatement("select * from test_table");
                        rs = ps.executeQuery();
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            System.out.println(rs.getInt(1));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        JDBCUtils.disconnect(connection, rs, ps);
                    }
                    return null;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
