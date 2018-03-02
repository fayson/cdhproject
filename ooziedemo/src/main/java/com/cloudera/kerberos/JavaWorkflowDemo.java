package com.cloudera.kerberos;

import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;
import java.util.Properties;

/**
 * package: com.cloudera.nokerberos
 * describe: 使用Oozie-client的API接口向Kerberos集群提交Java程序
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/03/02
 * creat_time: 下午20:56
 * 公众号：Hadoop实操
 */
public class JavaWorkflowDemo {
    private static String oozieURL = "http://ip-172-31-16-68.ap-southeast-1.compute.internal:11002/oozie";
    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("ssun.security.jgss.debug", "true"); //Kerberos Debug模式
        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/oozie-login.conf");

        AuthOozieClient oozieClient = new AuthOozieClient(oozieURL, AuthOozieClient.AuthType.KERBEROS.name());
        oozieClient.setDebugMode(1);

        try {
            Properties properties = oozieClient.createConfiguration();
            properties.put("oozie.wf.application.path", "${nameNode}/user/faysontest/oozie/javaaction");
            properties.put("oozie.use.system.libpath", "True");
            properties.put("nameNode", "hdfs://ip-172-31-16-68.ap-southeast-1.compute.internal:8020");
            properties.put("jobTracker", "ip-172-31-16-68.ap-southeast-1.compute.internal:8032");
            properties.put("mainClass", "org.apache.hadoop.examples.QuasiMonteCarlo");
            properties.put("arg1", "10");
            properties.put("arg2", "10");
            properties.put("javaOpts", "-Xmx1000m");
            properties.put("oozie.libpath", "${nameNode}/faysontest/jars/");

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
