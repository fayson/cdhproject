package com.cloudera;

import com.cloudera.utils.ClientUtils;
import com.cloudera.utils.KuduUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * package: com.cloudera
 * describe: 访问Kerberos环境下的Kudu
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/11/15
 * creat_time: 上午12:32
 * 公众号：Hadoop实操
 */
public class KuduKerberosExample {

    public static void main(String[] args) {
        ClientUtils.initKerberosENV(false);

        String kudu_master = System.getProperty("kuduMasters", "cdh1.fayson.com:7051,cdh2.fayson.com:7051,cdh3.fayson.com:7051");

        try {
            KuduClient kuduClient = UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<KuduClient>() {
                        @Override
                        public KuduClient run() throws Exception {
                            return new KuduClient.KuduClientBuilder(kudu_master).build();
                        }
                    }
            );

            String tableName = "user_info_kudu";

            //删除Kudu的表
            KuduUtils.dropTable(kuduClient, tableName);

            //创建一个Kudu的表
            KuduUtils.createTable(kuduClient, tableName);

            //列出Kudu下所有的表
            KuduUtils.tableList(kuduClient);

            //向Kudu指定的表中插入数据
            KuduUtils.upsert(kuduClient, tableName, 100);

            //扫描Kudu表中数据
            KuduUtils.scanerTable(kuduClient, tableName);

            try {
                kuduClient.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
