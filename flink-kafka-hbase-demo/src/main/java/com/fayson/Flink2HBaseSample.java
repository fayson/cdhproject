package com.fayson;

import com.fayson.customsink.HBaseSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * package: com.cloudera.hbase
 * describe: Flink从Socket读数据，将读取的数据解析后写入HBase示例
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2020/03/2020/3/19
 * creat_time: 9:40 下午
 * 公众号：Hadoop实操
 */
public class Flink2HBaseSample {

    private static String tableName = "flink_hbase";
    private static final String columnFamily = "info";

    public static void main(String[] args) throws Exception {

        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("hostname:" + hostname + ", port:" + port);

        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        DataStream<List<Put>> stream_d = text.countWindowAll(3).apply(new AllWindowFunction<String, List<Put>, GlobalWindow>() {
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

        env.execute("Flink2HBaseSample");

    }

}
