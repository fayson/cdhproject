package com.cloudera.hbase;

import com.sun.javafx.font.freetype.HBGlyphLayout;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * package: com.cloudera.hbase
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/11/30
 * creat_time: 上午12:49
 * 公众号：Hadoop实操
 */
public class KBHBaseTest {

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");

        Configuration configuration = getConfiguration();
        System.out.println(configuration.get("hbase.rootdir"));
        configuration.set("hadoop.security.authentication", "Kerberos");

        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab("fayson@CLOUDERA.COM", "/Volumes/Transcend/keytab/fayson.keytab");

            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("picHbase"));
            System.out.println(table.getName());

            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println(r.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 加载Hbase环境变量
     * @return
     */
    public static Configuration getConfiguration() {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("", "");
        configuration.set("", "");
        configuration.set("", "");
        configuration.set("", "");

        return  configuration;
    }
}
