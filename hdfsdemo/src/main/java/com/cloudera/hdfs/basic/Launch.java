package com.cloudera.hdfs.basic;

import com.cloudera.hdfs.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.File;
import java.io.IOException;

/**
 * package: com.cloudera.hdfs.basic
 * describe: 使用JAVA操作基本的HDFS API示例，用于POC测试
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/24
 * creat_time: 下午11:15
 * 公众号：Hadoop实操
 */
public class Launch {

    public static String confPath = System.getProperty("user.dir") + File.separator + "hdfs-conf";

    public static void main(String[] args) {
        //初始化HDFS Configuration 配置
        Configuration configuration = HDFSUtils.initConfiguration(confPath);

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            //获取操作类型
            String operation = args[0];

            switch (operation) {
                case "mkdir":
                    HDFSUtils.mkdir(fileSystem, args[1]);
                    break;
                case "upload":
                    HDFSUtils.uploadFile(fileSystem, args[1], args[2]);
                    break;
                case "createFile":
                    HDFSUtils.createFile(fileSystem, args[1], args[2]);
                    break;
                case "rename":
                    HDFSUtils.rename(fileSystem, args[1], args[2]);
                    break;
                case "readFile":
                    HDFSUtils.readFile(fileSystem, args[1]);
                    break;
                case "deleteFile":
                    HDFSUtils.delete(fileSystem, args[1]);
                    break;
                default:
                    System.out.print("操作类型错误");
                    break;
            }

            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}