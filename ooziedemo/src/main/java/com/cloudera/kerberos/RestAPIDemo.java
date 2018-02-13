package com.cloudera.kerberos;

import com.cloudera.utils.FileUtils;
import com.cloudera.utils.KBHttpUtils;

import java.io.File;
import java.util.HashMap;

/**
 * package: com.cloudera
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/12
 * creat_time: 下午10:40
 * 公众号：Hadoop实操
 */
public class RestAPIDemo {

    private static String oozieURL = "http://ip-172-31-16-68.ap-southeast-1.compute.internal:11000";
    private static String confPath = System.getProperty("user.dir") + File.separator + "ooziedemo" + File.separator + "conf" + File.separator;

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true"); //Kerberos Debug模式
        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/oozie-login.conf");


        HashMap<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/xml;charset=UTF-8");
        headers.put("Accept", "application/json");
        headers.put("X-Requested-By", "fayson");

        //获取Oozie版本
        String requestURL = oozieURL + "/oozie/versions";
        KBHttpUtils.getAccess(requestURL, headers);

        //获取系统状态
        requestURL = oozieURL + "/oozie/v1/admin/status";
        KBHttpUtils.getAccess(requestURL, headers);

        //创建一个workflow
        requestURL = oozieURL + "/oozie/v1/jobs?action=start";
        String requestxml = FileUtils.readToString(confPath + "workflow.xml");
        requestxml = requestxml.replaceAll("\\$USERNAME", "fayson").replaceAll("\\$NAMENODE", "hdfs://ip-172-31-16-68.ap-southeast-1.compute.internal:8020/user/bansalm/myapp/");
//        System.out.println(requestxml);
        KBHttpUtils.postAccess(requestURL, headers, requestxml);

        requestURL = oozieURL + "/oozie/v1/jobs?jobtype=wf";
        KBHttpUtils.getAccess(requestURL, headers);



    }
}
