package com.cloudera;

import com.cloudera.utils.KuduUtils;
import org.apache.kudu.client.*;

/**
 * package: com.cloudera
 * describe: 使用API方式访问Kudu数据库
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/12
 * creat_time: 上午2:04
 * 公众号：Hadoop实操
 */
public class KuduExample {

    private static final String KUDU_MASTER = System.getProperty("kuduMasters", "cdh1.fayson.com:7051,cdh2.fayson.com:7051,cdh3.fayson.com:7051");

    public static void main(String[] args) {
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        String tableName = "user_info";

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
    }
}
