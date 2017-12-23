package com.cloudera.hdfs.nonekerberos;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.FileInputStream;


/**
 * package: com.cloudera.hdfs.nonekerberos
 * describe: 使用HTTP的方式访问Hadoop集群的文件系统
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/21
 * creat_time: 下午11:59
 * 公众号：Hadoop实操
 */
public class HttpFsTest {

    public static void main(String[] args) {

        try {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet("http://cdh01:50070/webhdfs/v1/?user.name=fayson&op=LISTSTATUS");
            CloseableHttpResponse response = httpclient.execute(httpGet);
            HttpEntity entity = response.getEntity();

            System.out.println(EntityUtils.toString(entity));

            //向HDFS put文件

            HttpPost post = new HttpPost("http://ip-172-31-6-148.fayson.com:14000/webhdfs/v1/fayson1?user.name=hdfs&op=CREATE");

            FileBody binFileBody = new FileBody(new File("/Volumes/Transcend/work/cdhproject/kafkademo/target/kafka-demo-1.0-SNAPSHOT.jar"));

            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
            // add the file params
//            multipartEntityBuilder.addPart(serverFieldName, binFileBody);
            // 设置上传的其他参数
//            setUploadParams(multipartEntityBuilder, params);
            multipartEntityBuilder.setContentType(ContentType.TEXT_PLAIN);

            HttpEntity reqEntity = multipartEntityBuilder.build();
            post.setEntity(reqEntity);

            response = httpclient.execute(post);

            System.out.println(EntityUtils.toString(response.getEntity()));


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
