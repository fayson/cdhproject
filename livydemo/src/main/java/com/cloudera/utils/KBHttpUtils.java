package com.cloudera.utils;

import net.sourceforge.spnego.SpnegoHttpURLConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

/**
 * package: com.cloudera.utils
 * describe: 访问Kerberos环境的Http工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/2/12
 * creat_time: 下午4:57
 * 公众号：Hadoop实操
 */
public class KBHttpUtils {

    /**
     * HttpGET请求
     * @param url
     * @param headers
     * @return
     */
    public static String getAccess(String url, Map<String,String> headers) {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InputStream in = null;
        try {
            final SpnegoHttpURLConnection spnego = new SpnegoHttpURLConnection("Client");
            spnego.setRequestMethod("GET");
            if(headers != null && headers.size() > 0){
                headers.forEach((K,V)->spnego.setRequestProperty(K,V));
            }
            spnego.connect(new URL(url),bos);
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

        System.out.println("Result:" + sb.toString());

        return sb.toString();
    }

    /**
     * HttpDelete请求
     * @param url
     * @param headers
     * @return
     */
    public  static String deleteAccess(String url, Map<String,String> headers) {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InputStream in = null;
        try {
            final SpnegoHttpURLConnection spnego = new SpnegoHttpURLConnection("Client");
            spnego.setRequestMethod("DELETE");
            if(headers != null && headers.size() > 0){
                headers.forEach((K,V)->spnego.setRequestProperty(K,V));
            }
            spnego.connect(new URL(url),bos);
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

        System.out.println("Result:" + sb.toString());
        return sb.toString();
    }

    /**
     * HttpPost请求
     * @param url
     * @param headers
     * @param data
     * @return
     */
    public static String postAccess(String url, Map<String,String> headers, String data)  {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InputStream in = null;
        try {
            final SpnegoHttpURLConnection spnego = new SpnegoHttpURLConnection("Client");
            spnego.setRequestMethod("POST");
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

        System.out.println("Result:" + sb.toString());
        return sb.toString();

    }
}