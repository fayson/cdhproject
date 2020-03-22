package com.fayson.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * package: com.cloudera.hbase.utils
 * describe: 操作HBase工具类
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/11/17
 * creat_time: 下午5:08
 * 公众号：Hadoop实操
 */
public class HBaseUtils {


    /**
     * 获取HBase Connection
     * @param configuration
     * @return
     */
    public static Connection initConn(Configuration configuration) {
        Connection connection = null;

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connection;
    }

    /**
     * 获取库中所有的表
     * @param connection
     */
    public static void listTables(Connection connection){
        try {
            //获取所有的表名
            TableName[] tbs = connection.getAdmin().listTableNames();
            for (TableName tableName : tbs){
                System.out.println(tableName.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取HBase表中的数据
     * @param tname
     * @param connection
     */
    public static void readTable(String tname, Connection connection){
        try {
            TableName tableName = TableName.valueOf(tname);
            Admin admin = connection.getAdmin();
            //判断tname是否存在,存在就返回true,否则返回false
            Boolean flag = admin.tableExists(tableName);
            if(!flag) {
                System.out.println("表不存在");
                return;
            }
            //判断当前的表是否被禁用了,是就开启
            if (admin.isTableDisabled(tableName)){
                admin.enableTable(tableName);
            }

            Table table = connection.getTable(tableName);
            ResultScanner resultScanner = table.getScanner(new Scan());

            for (Result result:resultScanner){
                for (Cell cell:result.listCells()){
                    //取行健
                    String rowKey=Bytes.toString(CellUtil.cloneRow(cell));
                    //取到时间戳
                    long timestamp = cell.getTimestamp();
                    //取到族列
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    //取到修饰名
                    String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                    //取到值
                    String value = Bytes.toString(CellUtil.cloneValue(cell));

                    System.out.println("RowKey : " + rowKey + ",  Timestamp : " + timestamp + ", ColumnFamily : " + family + ", Key : " + qualifier + ", Value : " + value);
                }
            }
            resultScanner.close();
            table.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
