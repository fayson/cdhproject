package com.cloudera.kerberos;

import net.sourceforge.spnego.SpnegoHttpURLConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * package: com.cloudera
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/11
 * creat_time: 上午10:50
 * 公众号：Hadoop实操
 */
public class AppLivy {

    private static String LIVY_HOST = "http://ip-172-31-16-68.ap-southeast-1.compute.internal:8998/batches";

    public static void main(String[] args) {

        HashMap<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        headers.put("X-Requested-By", "fayson");

//        String kindJson = "{\"kind\":\"spark\"}";
        String submitJob = "{\"className\": \"org.apache.spark.examples.SparkPi\",\"executorMemory\": \"1g\",\"args\": [200],\"file\": \"/fayson-yarn/jars/spark-examples-1.6.0-cdh5.14.0-hadoop2.6.0-cdh5.14.0.jar\"}";


        System.out.println(postAccess(LIVY_HOST, headers, submitJob));

    }


    public static String postAccess(String url, Map<String,String> headers, String data)  {
        System.out.println("RestApiClient.postAccess:Kerberos模式");

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true"); //Kerberos Debug模式

        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/login-yarn.conf");
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InputStream in = null;
        try {
            final SpnegoHttpURLConnection spnego = new SpnegoHttpURLConnection("Client");
            spnego.setRequestMethod("POST");
            //spnego.setRequestProperty("Content-Type", "application/json");
            if(headers != null && headers.size() > 0){
                headers.forEach((K,V)->spnego.setRequestProperty(K,V));
            }
            if(data != null){
                bos.write(data.getBytes());
            }
            spnego.connect(new URL(url),bos);
            System.out.println("Kerberos data:"+data);
            System.out.println("HTTP Status Code: " + spnego.getResponseCode());
            System.out.println("HTTP Status Message: "+ spnego.getResponseMessage());

            in = spnego.getInputStream();
            byte[] b = new byte[1024];
            int len ;

            while ((len = in.read(b)) > 0) {
                sb.append(new String(b, 0, len));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();
    }
}
