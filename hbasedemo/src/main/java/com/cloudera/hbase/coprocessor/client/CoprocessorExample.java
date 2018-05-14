package com.cloudera.hbase.coprocessor.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * package: com.cloudera.hbase.coprocessor
 * describe: 客户端如何调用自定义的corprocessor类，Endpoint类型，该示例代码中介绍了几种调用的方式，以及各种调用方式的效率
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/9
 * creat_time: 下午11:30
 * 公众号：Hadoop实操
 */
public class CoprocessorExample {

    public static void main(String[] args) {

        //初始化HBase配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.setStrings("hbase.zookeeper.quorum", "ip-172-31-5-38.ap-southeast-1.compute.internal,ip-172-31-8-230.ap-southeast-1.compute.internal,ip-172-31-5-171.ap-southeast-1.compute.internal");
        try {
            //创建一个HBase的Connection
            Connection connection = ConnectionFactory.createConnection(configuration);

            Table testTable = connection.getTable(TableName.valueOf("TestTable"));
//            execBatchEndpointCoprocessor(testTable);
//            execEndpointCoprocessor(testTable);
            execFastEndpointCoprocessor(testTable);

            //关闭连接
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用batchCoprocessorService(MethodDescriptor var1, Message var2, byte[] var3, byte[] var4, R var5)的方法调用
     * 使用批量的方式，HBase会自动的将属于同一个RegionServer上的请求打包处理，可以节省网络交互的开销，效率会更高
     * @param table HBase表名
     * @return 返回表的总条数
     */
    public static long execBatchEndpointCoprocessor(Table table) {
        byte[] s= Bytes.toBytes("00000000000000000000000000");
        byte[] e= Bytes.toBytes("00000000000000000000000010");

        long start_t = System.currentTimeMillis();
        Map<byte[], ExampleProtos.CountResponse> batchMap = null;
        try {
            batchMap = table.batchCoprocessorService(
                    ExampleProtos.RowCountService.getDescriptor().findMethodByName("getKeyValueCount"),
                    ExampleProtos.CountRequest.getDefaultInstance(),
                    s,
                    e,
                    ExampleProtos.CountResponse.getDefaultInstance());
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        long batch_count = 0;
        System.out.println("Region Size:" + batchMap.size());
        for (ExampleProtos.CountResponse response : batchMap.values()) {
            batch_count += response.getCount();
        }
        System.out.println("方式一耗时：" + (System.currentTimeMillis() - start_t));
        System.out.println("方式一统计数量：" + batch_count);

        return batch_count;
    }

    /**
     * 通过HBase的coprocessorService(Class, byte[],byte[],Batch.Call)方法获取表的条数
     * @param table HBase 表对象
     * @return 返回表的条数
     */
    public static long execEndpointCoprocessor(Table table) {
        try {
            long start_t = System.currentTimeMillis();
            /**
             * coprocessorService(Class, byte[],byte[],Batch.Call)方法描述：
             * 参数一：Endpoint Coprocessor类，通过设置Endpoint Coprocessor类可以找到Region相应的协处理器
             * 参数二和参数三：要调用哪些Region上的服务则有startkey和endkey来决定，通过rowkey范围可以确定多个Region，如果设置为null则为所有的Region
             * 参数四：接口类Batch.Call定义如何调用协处理器，通过重写call()方法实现客户端的逻辑
             *
             * coprocessorService方法返回的是一个Map对象，Map的Key是Region的名字，Value是Batch.Call.call()方法的返回值
             */
            Map<byte[] , Long> map = table.coprocessorService(ExampleProtos.RowCountService.class, null, null, new Batch.Call<ExampleProtos.RowCountService, Long>() {

                @Override
                public Long call(ExampleProtos.RowCountService rowCountService) throws IOException {
                    ExampleProtos.CountRequest requet = ExampleProtos.CountRequest.getDefaultInstance();

                    BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback = new BlockingRpcCallback<>();
                    rowCountService.getKeyValueCount(null, requet, rpcCallback);
                    ExampleProtos.CountResponse response = rpcCallback.get();

                    return response.getCount();
                }
            });

            //对协处理器返回的所有Region的数量累加得出表的总条数
            long count = 0;
            System.out.println("Region Size:" + map.size());
            for(Long count_r : map.values()) {
                count += count_r;
            }
            System.out.println("方式二耗时：" + (System.currentTimeMillis() - start_t));
            System.out.println("方式二统计数量：" + count);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return 0l;
    }

    /**
     * 效率最高的方式，在方式二的基础上优化
     * 通过HBase的coprocessorService(Class, byte[],byte[],Batch.Call,Callback<R>)方法获取表的总条数
     * @param table HBase表名
     * @return 返回表的总条数
     */
    public static long execFastEndpointCoprocessor(Table table) {
        long start_t = System.currentTimeMillis();
        //定义总的 rowCount 变量
        AtomicLong totalRowCount = new AtomicLong();
        try {
            Batch.Callback<Long> callback = new Batch.Callback<Long>() {
                @Override
                public void update(byte[] region, byte[] row, Long result) {
                    totalRowCount.getAndAdd(result);
                }
            };

            table.coprocessorService(ExampleProtos.RowCountService.class, null, null, new Batch.Call<ExampleProtos.RowCountService, Long>() {
                @Override
                public Long call(ExampleProtos.RowCountService rowCountService) throws IOException {
                    ExampleProtos.CountRequest requet = ExampleProtos.CountRequest.getDefaultInstance();

                    BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback = new BlockingRpcCallback<>();
                    rowCountService.getKeyValueCount(null, requet, rpcCallback);
                    ExampleProtos.CountResponse response = rpcCallback.get();

                    return response.getCount();
                }
            }, callback);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.out.println("方式三耗时：" + (System.currentTimeMillis() - start_t));
        System.out.println("方式三统计数量：" + totalRowCount.longValue());

        return totalRowCount.longValue();
    }

}