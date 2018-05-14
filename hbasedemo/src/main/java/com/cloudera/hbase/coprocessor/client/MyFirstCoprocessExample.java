package com.cloudera.hbase.coprocessor.client;

import com.cloudera.hbase.coprocessor.server.MyFirstCoprocessor;
import com.cloudera.hbase.coprocessor.server.MyFirstCoprocessorEndPoint;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * package: com.cloudera.hbase.coprocessor.client
 * describe: 调用HBase RegionServer端的协处理器
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/14
 * creat_time: 下午6:36
 * 公众号：Hadoop实操
 */
public class MyFirstCoprocessExample {

    public static void main(String[] args) {
        String table_name = "fayson_coprocessor";

        //初始化HBase配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.setStrings("hbase.zookeeper.quorum", "ip-172-31-5-38.ap-southeast-1.compute.internal,ip-172-31-8-230.ap-southeast-1.compute.internal,ip-172-31-5-171.ap-southeast-1.compute.internal");
        try {
            //创建一个HBase的Connection
            Connection connection = ConnectionFactory.createConnection(configuration);
            TableName tableName = TableName.valueOf(table_name);
            if(!connection.getAdmin().tableExists(tableName)) {
                System.out.println(table_name + "does not exist....");
                System.exit(0);
            }
            Table table = connection.getTable(tableName);

            //删除表上的协处理器
            deleteCoprocessor(connection, table, MyFirstCoprocessorEndPoint.class);

            //为指定的表添加协处理器
            String hdfspath = "hdfs://nameservice3/hbase/coprocessor/hbase-demo-1.0-SNAPSHOT.jar";
            setupToExistTable(connection, table, hdfspath, MyFirstCoprocessorEndPoint.class);

            //客户端调用Region端的协处理器
            execFastEndpointCoprocessor(table, "info", "sales:MAX,sales:MIN,sales:AVG,sales:SUM,sales:COUNT");

            //关闭连接
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 删除HBase表上的协处理器
     * @param connection
     * @param table
     * @param cls
     */
    public static void deleteCoprocessor(Connection connection, Table table, Class<?>... cls) {
        System.out.println("begin delete " + table.getName().toString() + " Coprocessor......");
        try {
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();
            for(Class cass : cls) {
                hTableDescriptor.removeCoprocessor(cass.getCanonicalName());
            }
            connection.getAdmin().modifyTable(table.getName(), hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end delete " + table.getName().toString() + " Coprocessor......");
    }

    /**
     *
     * @param connection
     * @param table
     * @param jarPath
     * @param cls
     */
    public static void setupToExistTable(Connection connection, Table table, String jarPath, Class<?>... cls) {
        try {
            if(jarPath != null && !jarPath.isEmpty()) {
                Path path = new Path(jarPath);
                HTableDescriptor hTableDescriptor = table.getTableDescriptor();
                for(Class cass : cls) {
                    hTableDescriptor.addCoprocessor(cass.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);
                }
                connection.getAdmin().modifyTable(table.getName(), hTableDescriptor);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 效率最高的方式，在方式二的基础上优化
     * 通过HBase的coprocessorService(Class, byte[],byte[],Batch.Call,Callback<R>)方法获取表的总条数
     * @param table HBase表名
     * @return 返回表的总条数
     */
    public static long execFastEndpointCoprocessor(Table table, String family, String columns) {
        long start_t = System.currentTimeMillis();
        //定义总的 rowCount 变量
        AtomicLong totalRowCount = new AtomicLong();
        AtomicDouble maxValue = new AtomicDouble(Double.MIN_VALUE);
        AtomicDouble minValue = new AtomicDouble(Double.MAX_VALUE);
        AtomicDouble sumValue = new AtomicDouble();

        try {
            Batch.Callback<MyFirstCoprocessor.MyCoprocessResponse> callback = new Batch.Callback<MyFirstCoprocessor.MyCoprocessResponse>() {
                @Override
                public void update(byte[] bytes, byte[] bytes1, MyFirstCoprocessor.MyCoprocessResponse myCoprocessResponse) {
                    //更新Count值
                    totalRowCount.getAndAdd(myCoprocessResponse.getCount());

                    //更新最大值
                    if(myCoprocessResponse.getMaxnum() > maxValue.doubleValue()) {
                        maxValue.compareAndSet(maxValue.doubleValue(), myCoprocessResponse.getMaxnum());
                    }

                    //更新最小值
                    if(myCoprocessResponse.getMinnum() < minValue.doubleValue()) {
                        minValue.compareAndSet(minValue.doubleValue(), myCoprocessResponse.getMinnum());
                    }

                    //更新求和
                    sumValue.getAndAdd(myCoprocessResponse.getSumnum());
                }
            };

            table.coprocessorService(MyFirstCoprocessor.AggregationService.class, null, null, new Batch.Call<MyFirstCoprocessor.AggregationService, MyFirstCoprocessor.MyCoprocessResponse>() {
                @Override
                public MyFirstCoprocessor.MyCoprocessResponse call(MyFirstCoprocessor.AggregationService aggregationService) throws IOException {
                    MyFirstCoprocessor.MyCoprocessRequest requet = MyFirstCoprocessor.MyCoprocessRequest.newBuilder().setFamily(family).setColumns(columns).build();

                    BlockingRpcCallback<MyFirstCoprocessor.MyCoprocessResponse> rpcCallback = new BlockingRpcCallback<>();
                    aggregationService.getAggregation(null, requet, rpcCallback);
                    MyFirstCoprocessor.MyCoprocessResponse response = rpcCallback.get();

                    return response;
                }
            }, callback);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.out.println("耗时：" + (System.currentTimeMillis() - start_t));
        System.out.println("totalRowCount：" + totalRowCount.longValue());
        System.out.println("maxValue：" + maxValue.doubleValue());
        System.out.println("minValue：" + minValue.doubleValue());
        System.out.println("sumValue：" + sumValue.doubleValue());
        System.out.println("avg：" + new DoubleColumnInterpreter().divideForAvg(sumValue.doubleValue(), totalRowCount.longValue()));

        return totalRowCount.longValue();
    }
}
