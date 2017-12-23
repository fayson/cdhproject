package com.cloudera.hdfs.nonekerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

/**
 * package: com.cloudera.hdfs.nonekerberos
 * describe: 使用WebHDFSFileystem的方式访问Hadoop集群的文件系统
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/21
 * creat_time: 下午11:51
 * 公众号：Hadoop实操
 */
public class WebHDFSTest {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        UserGroupInformation.createRemoteUser("fayson");
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        try {
            webHdfsFileSystem.initialize(new URI("http://13.250.102.77:50070"), configuration);
            System.out.println(webHdfsFileSystem.getUri());
            //向HDFS Put文件
            webHdfsFileSystem.copyFromLocalFile(new Path("/Users/fayson/Desktop/run-kafka/"), new Path("/fayson1"));

            //列出HDFS根目录下的所有文件
            FileStatus[] fileStatuses =  webHdfsFileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath().getName());
            }

            webHdfsFileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
