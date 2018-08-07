package com.cloudera.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * package: com.cloudera
 * describe: Http请求工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/12
 * creat_time: 下午12:16
 * 公众号：Hadoop实操
 */
public class HttpUtils {

    /**
     * Get方式用户名和密码认证
     * @param url
     * @param headers
     * @param username
     * @param password
     * @return
     */
    public static String getAccessByAuth(String url, Map<String, String> headers, String username, String password) {
        String result = null;

        URI uri = URI.create(url);
        CredentialsProvider credsProvider = new BasicCredentialsProvider();

        credsProvider.setCredentials(new AuthScope(uri.getHost(), uri.getPort()),
                new UsernamePasswordCredentials(username, password));

        CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider)
                .build();

        HttpGet httpGet = new HttpGet(uri);
        if(headers != null && headers.size() > 0){
            headers.forEach((K,V)->httpGet.addHeader(K,V));
        }

        HttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            HttpEntity resultEntity = response.getEntity();
            result = EntityUtils.toString(resultEntity);

            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }

    /**
     * Post方式用户名和密码认证
     * @param url
     * @param headers
     * @param data
     * @param username
     * @param password
     * @return
     */
    public static  String postAccessByAuth(String url, Map<String, String> headers, String data, String username, String password) {
        String result = null;

        URI uri = URI.create(url);
        CredentialsProvider credsProvider = new BasicCredentialsProvider();

        credsProvider.setCredentials(new AuthScope(uri.getHost(), uri.getPort()),
                new UsernamePasswordCredentials(username, password));

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();

        HttpPost post = new HttpPost(uri);

        if(headers != null && headers.size() > 0){
            headers.forEach((K,V)->post.addHeader(K,V));
        }

        try {
            if(data != null) {
                StringEntity entity = new StringEntity(data);
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");
                post.setEntity(entity);
            }

            HttpResponse response = httpClient.execute(post);
            HttpEntity resultEntity = response.getEntity();
            result = EntityUtils.toString(resultEntity);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Put方式用户名和密码认证方式
     * @param url
     * @param headers
     * @param data
     * @param username
     * @param password
     * @return
     */
    public static  String putAccessByAuth(String url, Map<String, String> headers, String data, String username, String password) {
        String result = null;

        URI uri = URI.create(url);
        CredentialsProvider credsProvider = new BasicCredentialsProvider();

        credsProvider.setCredentials(new AuthScope(uri.getHost(), uri.getPort()),
                new UsernamePasswordCredentials(username, password));

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();

        HttpPut put = new HttpPut(uri);

        if(headers != null && headers.size() > 0){
            headers.forEach((K,V)->put.addHeader(K,V));
        }

        try {
            if(data != null) {
                StringEntity entity = new StringEntity(data);
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");
                put.setEntity(entity);
            }

            HttpResponse response = httpClient.execute(put);
            HttpEntity resultEntity = response.getEntity();
            result = EntityUtils.toString(resultEntity);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

}