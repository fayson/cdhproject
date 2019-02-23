package com.cloudera.hdfs.basic;

import com.cloudera.hdfs.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * package: com.cloudera.hdfs.basic
 * describe: Java客户端多集群访问操作(Kerberos和非Kerberos集群)
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2019/2/21
 * creat_time: 上午10:44
 * 公众号：Hadoop实操
 */
public class MultipleClusterTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "hdfsdemo" + File.separator + "local-kb-conf";
    private static String noconfPath = System.getProperty("user.dir") + File.separator + "hdfsdemo" + File.separator + "local-nokb-conf";

    public static void main(String[] args) {
        //初始化Kerberos环境HDFS Configuration 配置
        Configuration configuration = HDFSUtils.initConfiguration(confPath);
        configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        initKerberosENV(configuration);

        //初始化非Kerberos环境HDFS Configuration配置
        Configuration noKbConf = HDFSUtils.initConfiguration(noconfPath);
        try {
            FileSystem fileSystem = FileSystem.get(configuration);

            URI uri = FileSystem.getDefaultUri(noKbConf);
            FileSystem nokbfileSystem = FileSystem.newInstance(uri, noKbConf, UserGroupInformation.getCurrentUser().getShortUserName());

            //创建目录
            HDFSUtils.mkdir(fileSystem, "/test");
            HDFSUtils.mkdir(nokbfileSystem, "/test");

            HDFSUtils.mkdir(fileSystem, "/test1");
            HDFSUtils.mkdir(nokbfileSystem, "/test1");
            HDFSUtils.uploadFile(nokbfileSystem, "/Users/zoulihan/Desktop/hue.ini", "/test");
            HDFSUtils.uploadFile(fileSystem, "/Users/zoulihan/Desktop/hue.ini", "/test");

            nokbfileSystem.close();
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化Kerberos环境
     */
    public static void initKerberosENV(Configuration conf) {
        System.setProperty("java.security.krb5.conf", "/Users/zoulihan/Documents/develop/kerberos/local/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("fayson", "/Users/zoulihan/Documents/develop/kerberos/local/fayson.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
