package com.fayson;

import com.fayson.customsink.HBaseSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * package: com.cloudera.hbase
 * describe: Flink从非Kerberos的Kafka集群读取数据，写入Kerberos环境下的HBase集群
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2020/03/2020/3/24
 * creat_time: 10:14 上午
 * 公众号：Hadoop实操
 */
public class Kafka2HBaseSample {
    private static String tableName = "flink_hbase";
    private static final String columnFamily = "info";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.221:9092,192.168.0.222:9092,192.168.0.223:9092");
        properties.setProperty("group.id", "testGroup");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("security.protocol", "PLAINTEXT");

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test_topic", new SimpleStringSchema(), properties));

        DataStream<List<Put>> stream_d = stream.countWindowAll(3).apply(new AllWindowFunction<String, List<Put>, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<String> message, Collector<List<Put>> out) throws Exception {
                List<Put> putList=new ArrayList<Put>();
                for (String value : message) {
                    if(value.length() > 0) {
                        String rowKey = value.replace(",","-");
                        Put put = new Put(Bytes.toBytes(rowKey.toString()));
                        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("name"),Bytes.toBytes(value));
                        putList.add(put);
                    }
                }
                out.collect(putList);
            }
        });

        stream_d.addSink(new HBaseSink(tableName)).setParallelism(4);

        env.execute("Kafka2HBaseSample");
    }
}
