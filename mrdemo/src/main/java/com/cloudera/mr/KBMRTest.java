package com.cloudera.mr;

import com.cloudera.utils.ConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;

/**
 * package: com.cloudera.mr
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/4
 * creat_time: 下午11:37
 * 公众号：Hadoop实操
 */
public class KBMRTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "mrdemo" + File.separator + "conf";

    public static void main(String[] args) {
        try {
            System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            System.setProperty("sun.security.krb5.debug", "true"); //Kerberos Debug模式

            Configuration conf = ConfigurationUtil.getConfiguration(confPath);

            //登录Kerberos账号
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("fayson@CLOUDERA.COM", "/Volumes/Transcend/keytab/fayson.keytab");
            UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();

            Job wcjob = InitMapReduceJob.initWordCountJob(conf);
            wcjob.setJarByClass(KBMRTest.class);
            wcjob.setJobName("KBMRTest");

            //调用job对象的waitForCompletion()方法，提交作业。
            boolean res = wcjob.waitForCompletion(true);
            System.exit(res ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
