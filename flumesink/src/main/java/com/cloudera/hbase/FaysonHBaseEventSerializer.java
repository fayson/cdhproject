package com.cloudera.hbase;

import org.apache.flume.Event;
import org.apache.flume.sink.hbase.HbaseEventSerializer;

/**
 * package: com.cloudera.hbase
 * describe: 继承HBaseSink的HbaseEventSerializer接口类，增加initialize(Event var1, byte[] var2, String var3)
 * 用于处理指定rowkey
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/6/3
 * creat_time: 下午11:54
 * 公众号：Hadoop实操
 */
public interface FaysonHBaseEventSerializer extends HbaseEventSerializer {
    void initialize(Event var1, byte[] var2, String var3);
}