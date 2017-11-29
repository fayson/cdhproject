package com.cloudera.bean;

import org.apache.solr.client.solrj.beans.Field;

/**
 * package: com.cloudera.bean
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/11/25
 * creat_time: 下午8:32
 * 公众号：Hadoop实操
 */
public class Message {
    @Field
    private String id;
    @Field
    private String created_at;
    @Field
    private String text;
    @Field
    private String text_cn;

    public Message(String id, String created_at, String text, String text_cn) {
        this.id = id;
        this.created_at = created_at;
        this.text = text;
        this.text_cn = text_cn;
    }
}
