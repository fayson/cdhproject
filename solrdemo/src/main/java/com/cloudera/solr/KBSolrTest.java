package com.cloudera.solr;

import com.cloudera.bean.Message;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

/**
 * package: com.cloudera.solr
 * describe: Kerberos环境下的Solr访问
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/11/26
 * creat_time: 上午2:10
 * 公众号：Hadoop实操
 */
public class KBSolrTest {

    static final String zkHost = "ip-172-31-22-86.ap-southeast-1.compute.internal:2181/solr";
    static final String defaultCollection = "collection1";
    static final int socketTimeout = 20000;
    static final int zkConnectTimeout = 10000;

    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/jaas-client.conf");

        HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
        CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.setDefaultCollection(defaultCollection);
        cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
        cloudSolrServer.setZkClientTimeout(socketTimeout);
        cloudSolrServer.connect();

        addIndex(cloudSolrServer);

        addBeanIndex(cloudSolrServer);

        search(cloudSolrServer, "id:12345678955");
        search(cloudSolrServer, "id:12345678966");

        deleteIndex(cloudSolrServer, "12345678955");
        search(cloudSolrServer, "id:12345678955");

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
        } catch (SolrServerException e) {
            e.printStackTrace();
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
            solrInputDocument.setField("id", "12345678955");
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
     * @param solrClient
     */
    public static void addBeanIndex(CloudSolrServer solrClient) {
        try {
            Message message = new Message("12345678966", "2017-11-25 02:35:07", "hello world", "李四也是个农民，勤劳致富，奔小康");
            solrClient.addBean(message);

            solrClient.commit();
        } catch (Exception e) {
            System.out.println("Unknowned Exception!!!!!");
            e.printStackTrace();
        }
    }

    /**
     * 删除索引
     *
     * @param solrClient
     * @param id
     */
    public static void deleteIndex(CloudSolrServer solrClient, String id) {
        try {
            solrClient.deleteById(id);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
