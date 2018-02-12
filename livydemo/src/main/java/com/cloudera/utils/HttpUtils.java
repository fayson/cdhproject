package com.cloudera.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

/**
 * package: com.cloudera
 * describe: 封装非Kerberos环境的Http请求工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/12
 * creat_time: 下午12:16
 * 公众号：Hadoop实操
 */
public class HttpUtils {

    /**
     * HttpGET请求
     * @param url
     * @param headers
     * @return
     */
    public static String getAccess(String url, Map<String,String> headers) {
        String result = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        if(headers != null && headers.size() > 0){
            headers.forEach((K,V)->httpGet.addHeader(K,V));
        }
        try {
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * HttpDelete请求
     * @param url
     * @param headers
     * @return
     */
    public  static String deleteAccess(String url, Map<String,String> headers) {
        String result = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpDelete httpDelete = new HttpDelete(url);
        if(headers != null && headers.size() > 0){
            headers.forEach((K,V)->httpDelete.addHeader(K,V));
        }
        try {
            HttpResponse response = httpClient.execute(httpDelete);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * HttpPost请求
     * @param url
     * @param headers
     * @param data
     * @return
     */
    public static String postAccess(String url, Map<String,String> headers, String data)  {
        String result = null;

        CloseableHttpClient httpClient = HttpClients.createDefault();

        HttpPost post = new HttpPost(url);

        if(headers != null && headers.size() > 0){
            headers.forEach((K,V)->post.addHeader(K,V));
        }

        try {
            StringEntity entity = new StringEntity(data);
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            post.setEntity(entity);

            HttpResponse response = httpClient.execute(post);
            HttpEntity resultEntity = response.getEntity();
            result = EntityUtils.toString(resultEntity);

            System.out.println(result);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}