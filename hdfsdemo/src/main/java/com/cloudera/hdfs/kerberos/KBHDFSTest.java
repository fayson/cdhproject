package com.cloudera.hdfs.kerberos;

import com.cloudera.hdfs.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

/**
 * package: com.cloudera.hdfs.kerberos
 * describe: 访问Kerberos环境下的HDFS
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/2
 * creat_time: 下午11:54
 * 公众号：Hadoop实操
 */
public class KBHDFSTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "hdfsdemo" + File.separator + "kb-conf";

    public static void main(String[] args) {
        //初始化HDFS Configuration 配置
        Configuration configuration = HDFSUtils.initConfiguration(confPath);

        //初始化Kerberos环境
        initKerberosENV(configuration);
        try {
            FileSystem fileSystem = FileSystem.get(configuration);

            //创建目录
            HDFSUtils.mkdir(fileSystem, "/test");

            //上传本地文件至HDFS目录
//            HDFSUtils.uploadFile(fileSystem, "/Volumes/Transcend/keytab/schema.xml", "/test");

            //文件重命名
            HDFSUtils.rename(fileSystem, "/test/item.csv", "/test/fayson.csv");

            //查看文件
            HDFSUtils.readFile(fileSystem, "/test/fayson.csv");

            //删除文件
            HDFSUtils.delete(fileSystem, "/test/fayson.csv");

            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 初始化Kerberos环境
     */
    public static void initKerberosENV(Configuration conf) {
        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("fayson@CLOUDERA.COM", "/Volumes/Transcend/keytab/fayson.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
