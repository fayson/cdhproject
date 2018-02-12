package com.cloudera.kerberos;

import com.cloudera.utils.KBHttpUtils;
import java.util.HashMap;

/**
 * package: com.cloudera
 * describe: Kerberos环境下Livy RESTful API接口调用
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/11
 * creat_time: 上午10:50
 * 公众号：Hadoop实操
 */
public class AppLivy {

    private static String LIVY_HOST = "http://ip-172-31-21-83.ap-southeast-1.compute.internal:8998";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true"); //Kerberos Debug模式

        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/login-yarn.conf");

        HashMap<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        headers.put("X-Requested-By", "fayson");

        //创建一个交互式会话
        String kindJson = "{\"kind\": \"spark\", \"proxyUser\":\"fayson\"}";
//        KBHttpUtils.postAccess(LIVY_HOST + "/sessions", headers, kindJson);

        //执行code
        String code = "{\"code\":\"sc.parallelize(1 to 2).count()\"}";
//        KBHttpUtils.postAccess(LIVY_HOST + "/sessions/2/statements", headers, code);

        //删除会话
//        KBHttpUtils.deleteAccess(LIVY_HOST + "/sessions/3", headers);

        //封装提交Spark作业的JSON数据
        String submitJob = "{\"className\": \"org.apache.spark.examples.SparkPi\",\"executorMemory\": \"1g\",\"args\": [200],\"file\": \"/fayson-yarn/jars/spark-examples-1.6.0-cdh5.14.0-hadoop2.6.0-cdh5.14.0.jar\"}";
        //向集群提交Spark作业
//        KBHttpUtils.postAccess(LIVY_HOST + "/batches", headers, submitJob);

        //通过提交作业返回的SessionID获取具体作业的执行状态及APPID
        KBHttpUtils.getAccess(LIVY_HOST + "/batches/14", headers);

    }
}
