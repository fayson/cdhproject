//package com.cloudera.solr;
//
//import com.cloudera.bean.Message;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.solr.client.solrj.*;
//import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
//import org.apache.solr.client.solrj.impl.*;
//import org.apache.solr.client.solrj.request.GenericSolrRequest;
//import org.apache.solr.client.solrj.request.RequestWriter;
//import org.apache.solr.client.solrj.request.schema.SchemaRequest;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//import org.apache.solr.common.SolrInputDocument;
//import org.apache.solr.common.params.SolrParams;
//import org.apache.solr.common.util.ContentStream;
//
//import java.io.*;
//import java.security.PrivilegedAction;
//import java.util.Collection;
//
///**
// * package: com.cloudera
// * describe: Kerberos环境下连接Solr，基于sorlj7.1.0版本
// * creat_user: Fayson
// * email: htechinfo@163.com
// * creat_date: 2017/11/24
// * creat_time: 下午4:08
// * 公众号：Hadoop实操
// */
//public class KBSolrTest {
//
//
//    public static void main(String[] args) {
//
//        final String zkHost = "ip-172-31-22-86.ap-southeast-1.compute.internal:2181/solr";
//        final String  defaultCollection = "collection1";
//        final int  socketTimeout = 20000;
//        final int zkConnectTimeout = 1000;
//
//        try {
//            System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
//
//            //使用Kerberos账号登录
//            Configuration configuration = new Configuration();
//            configuration.set("hadoop.security.authentication" , "Kerberos");
//            UserGroupInformation. setConfiguration(configuration);
//            UserGroupInformation.loginUserFromKeytab("fayson@CLOUDERA.COM", "/Volumes/Transcend/keytab/fayson.keytab");
//            System.out.println(UserGroupInformation.getCurrentUser() + "------" + UserGroupInformation.getLoginUser());
//
//            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
//
//            System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/jaas-solr-client.conf");
//
//            loginUser.doAs(new PrivilegedAction<Object>(){
//
//                public Object run() {
//
//                    CloudSolrClient.Builder builder = new CloudSolrClient.Builder();
//                    builder.withZkHost(zkHost);
//                    builder.withSocketTimeout(socketTimeout);
//
//                    Krb5HttpClientBuilder.regenerateJaasConfiguration();
//                    SolrHttpClientBuilder solrHttpClientBuilder = new Krb5HttpClientBuilder().getBuilder();
//                    HttpClientUtil.setHttpClientBuilder(solrHttpClientBuilder);
//
//
//                    CloudSolrClient cloudSolrClient = builder.build();
//                    cloudSolrClient.setDefaultCollection(defaultCollection);
//                    cloudSolrClient.connect();
//
//                    search(cloudSolrClient, "id:999247449312");
//
//                    addIndex(cloudSolrClient);
//
//                    try {
//                        cloudSolrClient.close();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    return null;
//                }
//            });
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 查找
//     * @param solrClient
//     * @param String
//     */
//    public static void search(CloudSolrClient solrClient, String String) {
//        SolrQuery query = new SolrQuery();
//        query.setQuery(String);
//        try {
//            QueryResponse response = solrClient.query(query);
//            SolrDocumentList docs = response.getResults();
//
//            System.out.println("文档个数：" + docs.getNumFound());
//            System.out.println("查询时间：" + response.getQTime());
//
//            for (SolrDocument doc : docs) {
//                String id = (String) doc.getFieldValue("id");
//                String created_at = (String) doc.getFieldValue("created_at");
//                String text = (String) doc.getFieldValue("text");
//                String text_cn = (String) doc.getFieldValue("text_cn");
//                System.out.println("id: " + id);
//                System.out.println("created_at: " + created_at);
//                System.out.println("text: " + text);
//                System.out.println("text_cn: " + text_cn);
//                System.out.println();
//            }
//        } catch (SolrServerException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            System.out.println("Unknowned Exception!!!!");
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 添加索引，暂未测试通过
//     * @param solrClient
//     */
//    public static void addIndex(CloudSolrClient solrClient) {
//        try {
//
//            SolrInputDocument solrInputDocument = new SolrInputDocument();
//            solrInputDocument.setField("id", "1234567890");
////            solrInputDocument.setField("created_at", "2017-11-25 02:35:07");
////            solrInputDocument.setField("text", "hello world");
////            solrInputDocument.setField("text_cn", "张三是个农民，勤劳致富，奔小康");
//
////            solrClient.add("collection1", solrInputDocument);
//
//            Message message = new Message("1234567890", "2017-11-25 02:35:07","hello world","张三是个农民，勤劳致富，奔小康");
//            solrClient.addBean("collection1", message);
//
////            solrClient.commit();
//        } catch (SolrServerException e) {
//            System.out.println("Add docs Exception !!!");
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            System.out.println("Unknowned Exception!!!!!");
//            e.printStackTrace();
//        }
//    }
//}
