package com.fayson.customsink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import java.io.File;
import java.util.List;

/**
 * package: com.fayson.hbase
 * describe: 自定义HBase Sink
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2020/3/19
 * creat_time: 10:13 下午
 * 公众号：Hadoop实操
 */
public class HBaseSink extends RichSinkFunction<List<Put>> {

    private String tableName;
    private Connection conn;

    public HBaseSink(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration config = getConfiguration();
        conn = ConnectionFactory.createConnection(config);
    }

    @Override
    public void invoke(List<Put> putList, Context context) throws Exception {
        if(conn != null) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.put(putList);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        conn.close();
    }

    /**
     * 加载Hbase环境变量
     * @return
     */
    public static org.apache.hadoop.conf.Configuration getConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("/etc/hbase/conf" + File.separator + "core-site.xml"));
        configuration.addResource(new Path("/etc/hbase/conf" + File.separator + "hdfs-site.xml"));
        configuration.addResource(new Path("/etc/hbase/conf" + File.separator + "hbase-site.xml"));
        return  configuration;
    }

}
