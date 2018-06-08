package com.cloudera.hdfs.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;


/**
 * package: com.cloudera.hdfs.utils
 * describe: HDFSAdmin API 工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/6/8
 * creat_time: 上午11:01
 * 公众号：Hadoop实操
 */
public class HDFSAdminUtils {

    /**
     * 设置HDFS指定目录下文件总数
     * @param hdfsAdmin
     * @param path
     * @param quota
     */
    public static void setQuota(HdfsAdmin hdfsAdmin, Path path, long quota) {
        try {
            hdfsAdmin.setQuota(path, quota);
            System.out.println("成功设置HDFS的" + path.getName() + "目录文件数为:" + quota);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置HDFS指定目录目录空间大小
     * @param hdfsAdmin
     * @param path
     * @param quota
     */
    public static void setSpaceQuota(HdfsAdmin hdfsAdmin, Path path, long quota) {
        try {
            hdfsAdmin.setSpaceQuota(path, quota);
            System.out.println("成功设置HDFS的" + path.getName() + "目录空间大小为:" + quota);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 清除指定HDFS目录的所有配额限制
     * @param hdfsAdmin
     * @param path
     */
    public static void clrAllQuota(HdfsAdmin hdfsAdmin, Path path) {
        try {
            hdfsAdmin.clearSpaceQuota(path);
            hdfsAdmin.clearQuota(path);
            System.out.println("成功清除HDFS的" + path.getName() + "目录的配额限制");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
