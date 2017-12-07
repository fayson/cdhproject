package com.cloudera.mr;

import com.cloudera.utils.ConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;

/**
 * package: com.cloudera.mr
 * describe: 向非Kerberos集群提交MR作业
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/4
 * creat_time: 下午11:37
 * 公众号：Hadoop实操
 */
public class NodeKBMRTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "nonekb-yarn-conf";

    public static void main(String[] args) {

        try {
            Configuration conf = ConfigurationUtil.getConfiguration(confPath);

            Job wcjob = InitMapReduceJob.initWordCountJob(conf);
            wcjob.setJarByClass(NodeKBMRTest.class);
            wcjob.setJobName("NodeKBMRTest");

            //调用job对象的waitForCompletion()方法，提交作业。
            boolean res = wcjob.waitForCompletion(true);
            System.exit(res ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
