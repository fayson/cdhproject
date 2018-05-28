package com.cloudera.hbase

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * package: com.cloudera.streaming
  * describe: 基于Cloudera-clabs提供的HBaseContext实现HBase的读写等操作
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/1/7
  * creat_time: 上午11:43
  * 公众号：Hadoop实操
  */
object HBaseOperator {

  val zkHost = "cdh01.fayson.com";
  val zkPort = "2181"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkHost)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    val hbaseContext = new HBaseContext(sc, conf)



    readHbase(hbaseContext)
  }

  def readHbase(hbaseContext : HBaseContext) : RDD[String] = {
    val tableName = "picHbase"
    val scan = new Scan()
    scan.setCaching(1)

//    val picHbaseRDD = hbaseContext.hbaseScanRDD(tableName, scan)
//    val newPicHbaseRDD = picHbaseRDD.foreach(v => {
//      println(Bytes.toString(v._1))
//      println(Bytes.toString(v._2.get(0)._3))
//    })

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

    rdd.foreach(s => println(s))

    rdd
  }

}