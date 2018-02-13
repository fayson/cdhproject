package com.cloudera.nokerberos;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;
import java.util.Properties;

/**
 * package: com.cloudera.nokerberos
 * describe: 使用Oozie-client的API接口向非Kerberos集群提交Shell Action作业
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/13
 * creat_time: 下午11:10
 * 公众号：Hadoop实操
 */
public class ShellWorkflowDemo {
    private static String oozieURL = "http://ip-172-31-6-148.fayson.com:11000/oozie";

    public static void main(String[] args) {

        System.setProperty("user.name", "faysontest");
        OozieClient oozieClient = new OozieClient(oozieURL);
        try {
            System.out.println(oozieClient.getServerBuildVersion());

            Properties properties = oozieClient.createConfiguration();
            properties.put("oozie.wf.application.path", "${nameNode}/user/faysontest/oozie/shellaction");
            properties.put("oozie.use.system.libpath", "True");
            properties.put("nameNode", "hdfs://ip-172-31-10-118.fayson.com:8020");
            properties.put("jobTracker", "ip-172-31-6-148.fayson.com:8032");
            properties.put("exec", "${nameNode}//faysontest/jars/ooziejob.sh");
            properties.put("argument", "fayson");

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

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
