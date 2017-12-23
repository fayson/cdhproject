package com.cloudera.hdfs.nonekerberos;

import com.cloudera.hdfs.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;

/**
 * package: com.cloudera.hdfs.nonekerberos
 * describe: 访问非Kerberos环境下的HDFS
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/2
 * creat_time: 下午11:54
 * 公众号：Hadoop实操
 */
public class NoneKBHDFSTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "hdfsdemo" + File.separator + "conf";

    public static void main(String[] args) {
        //初始化HDFS Configuration 配置
        Configuration configuration = HDFSUtils.initConfiguration(confPath);
        try {
            FileSystem fileSystem = FileSystem.get(configuration);


            //创建目录
            HDFSUtils.mkdir(fileSystem, "/fayson1");

            HDFSUtils.uploadFile(fileSystem , "/Volumes/Transcend/work/cdhproject/kafkademo", "/fayson1");

            //创建文件
            HDFSUtils.createFile(fileSystem, "/fayson1/test.txt", "123testaaaaaaaaaa");

            //文件重命名
            HDFSUtils.rename(fileSystem, "/fayson1/test.txt", "/fayson/fayson.txt");

            //查看文件
            HDFSUtils.readFile(fileSystem, "/fayson1/fayson.txt");

            //删除文件
            HDFSUtils.delete(fileSystem, "/fayson1/fayson.txt");

            fileSystem.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
