package com.cloudera.hdfs.nonekerberos;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

/**
 * package: com.cloudera.hdfs.nonekerberos
 * describe: Java使用HttpClient访问启用SSL的HttpFS服务
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/1/13
 * creat_time: 下午9:56
 * 公众号：Hadoop实操
 */
public class HttpFSSSLDemo {

    private static String HTTPFS_HOST = "114.119.11.142";

    public static void main(String[] args) {

        try{
            SSLContext sslcontext = SSLContexts.custom()
                    .loadTrustMaterial(new File("/Volumes/Transcend/keytab/ssl/httpfs-server.keystore"), "123456".toCharArray(), new TrustSelfSignedStrategy()).build();
            // Allow TLSv1 protocol only
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    sslcontext,
                    new String[] { "TLSv1" },
                    null,
                    SSLConnectionSocketFactory.getDefaultHostnameVerifier());

            CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

            System.out.println("===========Case No.1  List resources==========");
            String getRequest =  "https://" + HTTPFS_HOST + ":14000/webhdfs/v1/?op=liststatus&user.name=hdfs";
            HttpGet httpget = new HttpGet(getRequest);
            System.out.println("executing request " + httpget.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(httpget);

            System.out.println("----------------------------------------");
            System.out.println(response.getStatusLine());
            HttpEntity entity = response.getEntity();
            BufferedReader br =
                    new BufferedReader(new InputStreamReader((entity.getContent())));
            String output;
            while ((output = br.readLine()) != null) {
                System.out.println(output);
            }
            EntityUtils.consume(entity);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
