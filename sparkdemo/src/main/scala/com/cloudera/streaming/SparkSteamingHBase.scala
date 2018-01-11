package com.cloudera.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * package: com.cloudera.streaming
  * describe: SparkStreaming读取HBase表数据并将数据写入HDFS
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/1/9
  * creat_time: 上午12:09
  * 公众号：Hadoop实操
  */
object SparkSteamingHBase {

  val zkHost = "ip-172-31-5-190.fayson.com";
  val zkPort = "2181"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSteamingTest")
    sparkConf.set("spark.streaming.receiverRestartDelay", "5000"); //设置Receiver启动频率，每5s启动一次
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))  //设置Spark时间窗口，每10s处理一次

    val rddStream = ssc.receiverStream(new MyReceiver(zkHost, zkPort))

    rddStream.saveAsTextFiles("/sparkdemo/test", "001")

    ssc.start()
    ssc.awaitTermination()
  }
}