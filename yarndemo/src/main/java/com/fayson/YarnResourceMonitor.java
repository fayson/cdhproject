package com.fayson;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * package: com.cloudera.hbase
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2020/04/2020/4/29
 * creat_time: 2:40 下午
 * 公众号：Hadoop实操
 */
public class YarnResourceMonitor {

    private static String confPath = System.getProperty("user.dir") + File.separator + "yarndemo" + File.separator + "conf";

    public static void main(String[] args) {
        Configuration configuration = initConfiguration(confPath);

        //初始化Kerberos环境
        initKerberosENV(configuration);

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();

        try {
            Set<String> applicationTypes = Sets.newHashSet();
            applicationTypes.add("MAPREDUCE");
            applicationTypes.add("SPARK");
            applicationTypes.add("Apache Flink");

            EnumSet<YarnApplicationState> applicationStates = EnumSet.noneOf(YarnApplicationState.class);
            applicationStates.add(YarnApplicationState.ACCEPTED);
            applicationStates.add(YarnApplicationState.SUBMITTED);
            applicationStates.add(YarnApplicationState.RUNNING);
            applicationStates.add(YarnApplicationState.NEW);
            applicationStates.add(YarnApplicationState.NEW_SAVING);

            List<ApplicationReport> queues = yarnClient.getApplications(applicationTypes, applicationStates);



            for(ApplicationReport report : queues) {

                List<ContainerReport> containerReportList = yarnClient.getContainers(report.getCurrentApplicationAttemptId());
                System.out.println(report.getTrackingUrl());

                for(ContainerReport containerReport : containerReportList) {

                    System.out.println(containerReport.getLogUrl());
                }


                ApplicationResourceUsageReport usageReport = report.getApplicationResourceUsageReport();

                System.out.println("Memory Use:" + usageReport.getUsedResources().getMemory());
                System.out.println("CPU Use:" + usageReport.getUsedResources().getVirtualCores());
                System.out.println("User:" + report.getUser());
                System.out.println("Yarn Pool:" + report.getQueue());
            }

        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 初始化HDFS Configuration
     * @return configuration
     */
    public static Configuration initConfiguration(String confPath) {
        Configuration configuration = new Configuration();
        System.out.println(confPath + File.separator + "core-site.xml");
        configuration.addResource(new Path(confPath + File.separator + "core-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "hdfs-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "yarn-site.xml"));
        return configuration;
    }

    /**
     * 初始化Kerberos环境
     */
    public static void initKerberosENV(Configuration conf) {
        System.setProperty("java.security.krb5.conf", "/Users/taopanlong/Documents/develop/kerberos/local/234/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "false");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("cdhadmin@PREST.COM", "/Users/taopanlong/Documents/develop/kerberos/local/234/cdhadmin.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
