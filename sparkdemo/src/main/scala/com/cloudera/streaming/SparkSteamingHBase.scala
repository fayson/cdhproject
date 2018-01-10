package com.cloudera.streaming

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer

/**
  * package: com.cloudera.streaming
  * describe: TODO
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/1/9
  * creat_time: 上午12:09
  * 公众号：Hadoop实操
  */
object SparkSteamingHBase {

  val zkHost = "cdh.macro.com";
  val zkPort = "2181"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSteamingTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(20))

//    val rddStream = ssc.receiverStream(new MyReceiver(sc))
    val rddStream = ssc.receiverStream(new MyReceiver(zkHost, zkPort))

    rddStream.foreachRDD{rdd =>{
      rdd.foreach(s => println(s))
            rdd.saveAsTextFile("/sparkdemo1/test")
    }}
    rddStream.saveAsTextFiles("/sparkdemo/test", "001")

    ssc.start()
    ssc.awaitTermination()
  }

  def readHbase(sc: SparkContext) : RDD[String] = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkHost)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    val hbaseContext = new HBaseContext(sc, conf)

    val tableName = "picHbase"
    val scan = new Scan()
    scan.setCaching(1)

    val rdd = hbaseContext.hbaseRDD(tableName, scan, convert => {
      val b = new StringBuilder
      b.append(Bytes.toString(convert._2.getRow))
      b.append(",")

      val it = convert._2.listCells().iterator()
      while (it.hasNext) {
        val kv = it.next()
        b.append(Bytes.toString(kv.getValue))
        b.append(",")
        b.append(kv.getTimestamp)
      }

      b.toString()
    })

    rdd
  }

}
