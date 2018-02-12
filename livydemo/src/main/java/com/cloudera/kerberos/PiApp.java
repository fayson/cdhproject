package com.cloudera.kerberos;

import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;
import com.cloudera.livy.client.common.ClientConf;

import java.io.File;
import java.net.URI;
import java.util.Properties;

/**
 * package: com.cloudera
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/11
 * creat_time: 上午2:17
 * 公众号：Hadoop实操
 */
public class PiApp {
    private static String LIVY_HOST = "http://ip-172-31-21-83.ap-southeast-1.compute.internal:8998";
    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.err.println("Usage: PiJob <livy url> <slices>");
//            System.exit(-1);
//        }

//        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
//        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true"); //Kerberos Debug模式
//
//        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/login-yarn.conf");

        Properties props = new Properties();
        props.put("livy.client.http.spnego.enable", true);
        props.put("livy.client.http.auth.login.config", "/Volumes/Transcend/keytab/login-yarn.conf");
        props.put("livy.client.http.krb5.debug", true);
        props.put("livy.client.http.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        props.put("livy.client.http.spnego.enable", "true");

        LivyClient client = new LivyClientBuilder().setAll(props)
                .setURI(new URI(LIVY_HOST))//.setConf("livy.client.http.spnego.enable", "true")
                .build();

//        try {
//            System.out.println("Uploading livy-example jar to the SparkContext...");
//
//            client.uploadJar(new File("/Volumes/Transcend/keytab/livy-examples-0.4.0-SNAPSHOT.jar")).get();
//
//            final int slices = Integer.parseInt("10");
//            double pi = client.submit(new PiJob(slices)).get();
//
//            System.out.println("Pi is roughly " + pi);
//        } finally {
//            client.stop(true);
//        }
    }
}
