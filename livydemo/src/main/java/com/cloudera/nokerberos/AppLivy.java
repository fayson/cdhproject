package com.cloudera.nokerberos;

import com.cloudera.utils.HttpUtils;
import java.util.HashMap;

/**
 * package: com.cloudera
 * describe: 通过Java代码调用Livy的RESTful API实现向非Kerberos的CDH集群作业提交
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/11
 * creat_time: 上午10:50
 * 公众号：Hadoop实操
 */
public class AppLivy {

    private static String LIVY_HOST = "http://ip-172-31-7-172.fayson.com:8998";

    public static void main(String[] args) {
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        headers.put("X-Requested-By", "fayson");

        //创建一个交互式会话
//        String kindJson = "{\"kind\": \"spark\", \"proxyUser\":\"fayson\"}";
//        HttpUtils.postAccess(LIVY_HOST + "/sessions", headers, kindJson);

        //执行code
//        String code = "{\"code\":\"sc.parallelize(1 to 2).count()\"}";
//        HttpUtils.postAccess(LIVY_HOST + "/sessions/1/statements", headers, code);

        //删除会话
//        HttpUtils.deleteAccess(LIVY_HOST + "/sessions/2", headers);


        //封装提交Spark作业的JSON数据
        String submitJob = "{\"className\": \"org.apache.spark.examples.SparkPi\",\"executorMemory\": \"1g\",\"args\": [200],\"file\": \"/fayson-yarn/jars/spark-examples-1.6.0-cdh5.13.1-hadoop2.6.0-cdh5.13.1.jar\", \"proxyUser\":\"fayson\"}";
        //向集群提交Spark作业
//        HttpUtils.postAccess(LIVY_HOST + "/batches", headers, submitJob);

        //通过提交作业返回的SessionID获取具体作业的执行状态及APPID
        HttpUtils.getAccess(LIVY_HOST + "/batches/4", headers);

    }

}
