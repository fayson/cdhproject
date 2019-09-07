package com.cloudera.utils;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.net.URI;

/**
 * package: com.fayson
 * describe: 封装非Kerberos环境的Http请求工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2019/07/07
 * creat_time: 下午12:16
 * 公众号：Hadoop实操
 */
public class HttpUtils {

    /**
     * 使用账号密码认证的方式GET请求
     * @param url
     * @param username
     * @param password
     * @return
     * @throws Exception
     */
    public static String getAccess(String url, String username, String password) throws Exception {
        String result = null;

        URI uri = new URI(url);


        HttpHost target = new HttpHost(uri.getHost(),  uri.getPort(), uri.getScheme());

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope(target.getHostName(), target.getPort()),
                new UsernamePasswordCredentials(username, password));
        CloseableHttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider).build();
        try {

            // Create AuthCache instance
            AuthCache authCache = new BasicAuthCache();
            // Generate BASIC scheme object and add it to the local
            // auth cache
            BasicScheme basicAuth = new BasicScheme();
            authCache.put(target, basicAuth);

            // Add AuthCache to the execution context
            HttpClientContext localContext = HttpClientContext.create();
            localContext.setAuthCache(authCache);

            HttpGet httpget = new HttpGet(url);

            System.out.println("Executing request " + httpget.getRequestLine() + " to target " + target);
            CloseableHttpResponse response = httpclient.execute(target, httpget, localContext);
            try {
                System.out.println("----------------------------------------");
                System.out.println(response.getStatusLine());
                result = EntityUtils.toString(response.getEntity());
            } finally {
                response.close();
            }
        } finally {
            httpclient.close();
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        String aa = "\0\0\2\0\2";
        aa = aa.replace((char)2, (char)0);
        for(int i = 0; i < aa.length(); i++) {
            System.out.println(aa.charAt(i));
        }

        System.out.println((char)1);
    }
}