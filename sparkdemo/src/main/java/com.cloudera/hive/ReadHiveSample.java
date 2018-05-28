package com.cloudera.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * package: hive
 * describe: 使用Spark API访问Hive表
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/16
 * creat_time: 下午11:51
 * 公众号：Hadoop实操
 */
public class ReadHiveSample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
//                .setMaster("local[2]")
                .setMaster("yarn-client")
                .setAppName("AccessHive");

        SparkContext sparkContext = new SparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(sparkContext);
        DataFrame dataFrame = hiveContext.sql("show tables");
        dataFrame.show();

        dataFrame = hiveContext.sql("select * from test");

        dataFrame.show();
    }
}