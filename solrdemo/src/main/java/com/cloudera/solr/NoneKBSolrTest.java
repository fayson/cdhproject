package com.cloudera.solr;

import com.cloudera.bean.Message;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import java.io.IOException;

/**
 * package: com.cloudera.solr
 * describe: 使用Solrj4.10.3-cdh5.11.2版本访问非Kerberos环境下的Solr集群
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/11/26
 * creat_time: 上午12:08
 * 公众号：Hadoop实操
 */
public class NoneKBSolrTest {

    static final String zkHost = "13.229.70.204:2181/solr";
    static final String defaultCollection = "collection1";
    static final int socketTimeout = 20000;
    static final int zkConnectTimeout = 1000;

    public static void main(String[] args) {

        CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.setDefaultCollection(defaultCollection);
        cloudSolrServer.setZkClientTimeout(zkConnectTimeout);
        cloudSolrServer.setZkConnectTimeout(socketTimeout);

        cloudSolrServer.connect();

        search(cloudSolrServer, "id:12345678911");

        addIndex(cloudSolrServer);

        deleteIndex(cloudSolrServer, "12345678955");
    }

    /**
     * 查找
     *
     * @param solrClient
     * @param String
     */
    public static void search(CloudSolrServer solrClient, String String) {
        SolrQuery query = new SolrQuery();
        query.setQuery(String);
        try {
            QueryResponse response = solrClient.query(query);
            SolrDocumentList docs = response.getResults();

            System.out.println("文档个数：" + docs.getNumFound());
            System.out.println("查询时间：" + response.getQTime());

            for (SolrDocument doc : docs) {
                String id = (String) doc.getFieldValue("id");
                String created_at = (String) doc.getFieldValue("created_at");
                String text = (String) doc.getFieldValue("text");
                String text_cn = (String) doc.getFieldValue("text_cn");
                System.out.println("id: " + id);
                System.out.println("created_at: " + created_at);
                System.out.println("text: " + text);
                System.out.println("text_cn: " + text_cn);
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Unknowned Exception!!!!");
            e.printStackTrace();
        }
    }

    /**
     * 添加索引
     *
     * @param solrClient
     */
    public static void addIndex(CloudSolrServer solrClient) {
        try {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            solrInputDocument.setField("id", "1234567890");
            solrInputDocument.setField("created_at", "2017-11-25 02:35:07");
            solrInputDocument.setField("text", "hello world");
            solrInputDocument.setField("text_cn", "张三是个农民，勤劳致富，奔小康");
            solrClient.add(solrInputDocument);
            solrClient.commit();
        } catch (Exception e) {
            System.out.println("Unknowned Exception!!!!!");
            e.printStackTrace();
        }
    }

    /**
     * 使用JavaBean对象向Solr集群创建索引
     *
     * @param solrServer
     */
    public static void addBean(CloudSolrServer solrServer) {

        Message message = new Message("12345678911", "2017-11-25 02:35:07", "hello world", "张三是个农民，勤劳致富，奔小康");
        try {
            solrServer.addBean(message);
            solrServer.commit();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除指定Collection中的Index
     *
     * @param solrServer
     * @param id
     */
    public static void deleteIndex(CloudSolrServer solrServer, String id) {
        try {
            solrServer.deleteById(id);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
