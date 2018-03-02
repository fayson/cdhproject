package com.cloudera.kerberos;

import org.apache.oozie.client.*;

import java.util.List;
import java.util.Properties;

/**
 * package: com.cloudera.nokerberos
 * describe: 使用Oozie-client的API接口向Kerberos集群提交Spark程序
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/23
 * creat_time: 上午10:20
 * 公众号：Hadoop实操
 */
public class SparkWorkflowDemo {
    private static String oozieURL = "http://ip-172-31-16-68.ap-southeast-1.compute.internal:11000/oozie";
    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("ssun.security.jgss.debug", "true"); //Kerberos Debug模式
        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/oozie-login.conf");

        AuthOozieClient oozieClient = new AuthOozieClient(oozieURL, AuthOozieClient.AuthType.KERBEROS.name());
        oozieClient.setDebugMode(1);

        try {
//            System.out.println(oozieClient.getServerBuildVersion());

            Properties properties = oozieClient.createConfiguration();
            properties.put("oozie.wf.application.path", "${nameNode}/user/fayson/oozie/testoozie");
            properties.put("name", "MyfirstSpark");
            properties.put("nameNode", "hdfs://ip-172-31-16-68.ap-southeast-1.compute.internal:8020");
            properties.put("oozie.use.system.libpath", "True");
            properties.put("master", "yarn-cluster");
            properties.put("mode", "cluster");
            properties.put("class", "org.apache.spark.examples.SparkPi");
            properties.put("arg", "50");
            properties.put("sparkOpts", "--num-executors 4 --driver-memory 1g --driver-cores 1 --executor-memory 1g --executor-cores 1");
            properties.put("jar", "${nameNode}/fayson/jars/spark-examples-1.6.0-cdh5.13.1-hadoop2.6.0-cdh5.13.1.jar");
            properties.put("oozie.libpath", "${nameNode}/fayson/jars");
            properties.put("jobTracker", "ip-172-31-16-68.ap-southeast-1.compute.internal:8032");
            properties.put("file", "${nameNode}/fayson/jars");

            //运行workflow
            String jobid = oozieClient.run(properties);
            System.out.println(jobid);

            //等待10s
            new Thread(){
                public void run() {
                    try {
                        Thread.sleep(10000l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }.start();

            //根据workflow id获取作业运行情况
            WorkflowJob workflowJob = oozieClient.getJobInfo(jobid);
            //获取作业日志
            System.out.println(oozieClient.getJobLog(jobid));

            //获取workflow中所有ACTION
            List<WorkflowAction> list = workflowJob.getActions();
            for (WorkflowAction action : list) {
                //输出每个Action的 Appid 即Yarn的Application ID
                System.out.println(action.getExternalId());
            }

            //杀掉作业
//            oozieClient.kill(jobid);

        } catch (OozieClientException e) {
            e.printStackTrace();
        }
    }
}
