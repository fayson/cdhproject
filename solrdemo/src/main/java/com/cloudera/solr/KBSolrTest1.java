//package com.cloudera.solr;
//
//import com.cloudera.bean.Message;
//import org.apache.commons.codec.binary.Base64;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.http.*;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.Credentials;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.client.methods.HttpUriRequest;
//import org.apache.http.client.params.AuthPolicy;
//import org.apache.http.impl.auth.SPNegoSchemeFactory;
//import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.protocol.HttpContext;
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.CloudSolrServer;
//import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//import sun.security.acl.PrincipalImpl;
//
//import java.io.IOException;
//import java.security.Principal;
//import java.security.PrivilegedAction;
//
///**
// * package: com.cloudera.solr
// * describe: TODO
// * creat_user: Fayson
// * email: htechinfo@163.com
// * creat_date: 2017/11/25
// * creat_time: 下午11:40
// * 公众号：Hadoop实操
// */
//public class KBSolrTest1 {
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
//
//            System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/http-login.conf");
//            System.setProperty("sun.security.krb5.debug", "true");
////            System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
//
//            loginUser.doAs(new PrivilegedAction<Object>(){
//
//                public Object run() {
//
//                    DefaultHttpClient client = new DefaultHttpClient();
//                    //采用拦截器加权限验证
//                    client.addRequestInterceptor(new HttpRequestInterceptor() {
//                        public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
//                            String enc = "fayson@CLOUDERA.COM" + ":" + "123456";
//                            request.addHeader("Authorization", "Basic " + Base64.encodeBase64String(enc.getBytes()));
//                        }
//                    });
//
//                    //采用robin环方式负载
//                    LBHttpSolrServer server = new LBHttpSolrServer(client,
//                            "http://ec2-13-229-49-186.ap-southeast-1.compute.amazonaws.com:8983/solr/" + defaultCollection);
//
//                    CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost, server);
//                    cloudSolrServer.setDefaultCollection(defaultCollection);
//                    cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
//                    cloudSolrServer.setZkClientTimeout(socketTimeout);
//                    cloudSolrServer.connect();
//
//                    search(cloudSolrServer, "id:999247449312");
//
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
//    public static void search(CloudSolrServer solrClient, String String) {
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
//    public static void addIndex(CloudSolrServer solrClient) {
//        try {
//
////            SolrInputDocument solrInputDocument = new SolrInputDocument();
////            solrInputDocument.setField("id", "1234567890");
////            solrInputDocument.setField("created_at", "2017-11-25 02:35:07");
////            solrInputDocument.setField("text", "hello world");
////            solrInputDocument.setField("text_cn", "张三是个农民，勤劳致富，奔小康");
////            solrClient.add("collection1", solrInputDocument);
//
//            Message message = new Message("1234567890", "2017-11-25 02:35:07","hello world","张三是个农民，勤劳致富，奔小康");
//            solrClient.addBean(message);
//            solrClient.commit();
//
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
