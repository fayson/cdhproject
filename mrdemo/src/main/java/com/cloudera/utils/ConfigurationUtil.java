package com.cloudera.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;

/**
 * package: com.cloudera.utils
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/4
 * creat_time: 下午11:54
 * 公众号：Hadoop实操
 */
public class ConfigurationUtil {
    /**
     * 获取Hadoop配置信息
     * @param confPath
     * @return
     */
    public static Configuration getConfiguration(String confPath) {
        Configuration configuration = new YarnConfiguration();
        configuration.addResource(new Path(confPath + File.separator + "core-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "hdfs-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "mapred-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "yarn-site.xml"));
        configuration.setBoolean("dfs.support.append", true);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        return configuration;
    }
}
