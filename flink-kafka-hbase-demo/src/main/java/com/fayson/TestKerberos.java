package com.fayson;

import com.fayson.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

/**
 * package: com.cloudera.hbase
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2020/03/2020/3/21
 * creat_time: 10:51 下午
 * 公众号：Hadoop实操
 */
public class TestKerberos {

    private static String zk_hosts = "cdh1.prest.com:2181,cdh2.prest.com:2181,cdh3.prest.com:2181";
    private static Connection conn;

    public static void main(String[] args) throws IOException {
        System.setProperty("java.security.krb5.conf", "/Users/taopanlong/Documents/develop/kerberos/local/234/krb5.conf");
        org.apache.hadoop.conf.Configuration config = getConfiguration();
//        config.set(HConstants.ZOOKEEPER_QUORUM, zk_hosts);
//        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
//        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
//        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        config.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab("cdhadmin@PREST.COM", "/Users/taopanlong/Documents/develop/kerberos/local/234/cdhadmin.keytab");
        System.out.println(UserGroupInformation.getLoginUser());
        conn = ConnectionFactory.createConnection(config);

        Put put = new Put(Bytes.toBytes("1-fayson".toString()));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("1,fayson"));
        conn.getTable(TableName.valueOf("flink_hbase")).put(put);
        HBaseUtils.readTable("flink_hbase", conn);

    }

    /**
     * 加载Hbase环境变量
     * @return
     */
    public static Configuration getConfiguration() {

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("/Users/taopanlong/Documents/develop/kerberos/local/234/hbase-conf" + File.separator + "core-site.xml"));
        configuration.addResource(new Path("/Users/taopanlong/Documents/develop/kerberos/local/234/hbase-conf" + File.separator + "hdfs-site.xml"));
        configuration.addResource(new Path("/Users/taopanlong/Documents/develop/kerberos/local/234/hbase-conf" + File.separator + "hbase-site.xml"));

        return  configuration;
    }

    /**
     * 用于定时relogin Kerberos账号，防止Kerberos过期
     */
    class ReloginKerberos implements Runnable {

        @Override
        public void run() {
            while(true) {
                try { //每天执行一次
                    System.out.println(conn.isClosed());
                    UserGroupInformation.loginUserFromKeytab("cdhadmin@PREST.COM", "/Users/taopanlong/Documents/develop/kerberos/local/234/cdhadmin.keytab");
                    System.out.println(UserGroupInformation.getLoginUser());
                    Thread.sleep( 30 * 1000);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
