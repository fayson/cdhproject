//package com.cloudera.solr;
//
//import com.cloudera.bean.Message;
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.CloudSolrClient;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//
//import java.io.IOException;
//
///**
// * package: com.cloudera
// * describe: 非Kerberos环境下连接Solr，基于sorlj7.1.0版本
// * creat_user: Fayson
// * email: htechinfo@163.com
// * creat_date: 2017/11/24
// * creat_time: 下午10:12
// * 公众号：Hadoop实操
// */
//public class NoneKBSolrTest {
//
//    static final String zkHost = "13.229.70.204:2181/solr";
//    static final String  defaultCollection = "collection1";
//    static final int  socketTimeout = 20000;
//    static final int zkConnectTimeout = 1000;
//
//    public static void main(String[] args) {
//
//        CloudSolrClient.Builder builder = new CloudSolrClient.Builder();
//        builder.withZkHost(zkHost);
//        builder.withSocketTimeout(socketTimeout);
//
//
//        CloudSolrClient cloudSolrClient = builder.build();
//        cloudSolrClient.connect();
//        cloudSolrClient.setDefaultCollection(defaultCollection);
//
//        System.out.println(cloudSolrClient.getZkHost());
//
//        search(cloudSolrClient, "id:100037537918");
//
//        addIndex(cloudSolrClient);
//
//        try {
//            cloudSolrClient.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
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
//     * 添加索引
//     * @param solrClient
//     */
//    public static void addIndex(CloudSolrClient solrClient) {
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
//            solrClient.addBean("collection1", message);
//
//            solrClient.commit();
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
