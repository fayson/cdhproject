package com.cloudera.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * package: com.cloudera.hbase.utils
 * describe: 访问HBase客户端工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/11/17
 * creat_time: 下午4:55
 * 公众号：Hadoop实操
 */
public class ClientUtils {

    /**
     * 初始化访问HBase访问
     */
    public static Configuration initHBaseENV() {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource(ClientUtils.class.getClass().getResourceAsStream("/hbase-conf/core-site.xml"));
            configuration.addResource(ClientUtils.class.getClass().getResourceAsStream("/hbase-conf/hdfs-site.xml"));
            configuration.addResource(ClientUtils.class.getClass().getResourceAsStream("/hbase-conf/hbase-site.xml"));

            return  configuration;
        } catch(Exception e) {
            e.printStackTrace();
        }

        return  null;
    }
}
