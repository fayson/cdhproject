package com.cloudera.streaming

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.HBaseUtil

import scala.util.Try
import scala.util.parsing.json.JSON

/**
  * package: com.cloudera.streaming
  * describe: SparkStreaming 应用实时读取Kafka数据，解析后存入HBase
  * 使用spark-submit的方式提交作业
    spark-submit --class com.cloudera.streaming.Kafka2Spark2HBase \
    --master yarn-client --num-executors 1 --driver-memory 1g \
    --driver-cores 1 --executor-memory 1g --executor-cores 1 \
    spark-demo-1.0-SNAPSHOT.jar
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/5/28
  * creat_time: 上午10:09
  * 公众号：Hadoop实操
  */
object Kafka2Spark2HBase {

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf/0283.properties"

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if(!file.exists()) {
      System.out.println(Kafka2Spark2HBase.getClass.getClassLoader.getResource("0283.properties"))
      val in = Kafka2Spark2HBase.getClass.getClassLoader.getResourceAsStream("0283.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }

    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val zkHost = properties.getProperty("zookeeper.list")
    val zkport = properties.getProperty("zookeeper.port")
    System.out.println("kafka.brokers:" + brokers)
    System.out.println("kafka.topics:" + topics)
    System.out.println("zookeeper.list:" + zkHost)
    System.out.println("zookeeper.port:" + zkport)
    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics) || StringUtils.isEmpty(zkHost) || StringUtils.isEmpty(zkport)) {
      System.out.println("未配置Kafka和Zookeeper信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet

    val sparkConf = new SparkConf().setAppName("Kafka2Spark2HBase")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    dStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val connection = HBaseUtil.getHBaseConn(zkHost, zkport) // 获取Hbase连接
        partitionRecords.foreach(line => {
          //将Kafka的每一条消息解析为JSON格式数据
          println(line._2)
          val jsonObj =  JSON.parseFull(line._2)
          val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String, Any]]

          val rowkey = map.get("id").get.asInstanceOf[String]
          val name = map.get("name").get.asInstanceOf[String]
          val sex = map.get("sex").get.asInstanceOf[String]
          val city = map.get("city").get.asInstanceOf[String]
          val occupation = map.get("occupation").get.asInstanceOf[String]
          val mobile_phone_num = map.get("mobile_phone_num").get.asInstanceOf[String]
          val fix_phone_num = map.get("fix_phone_num").get.asInstanceOf[String]
          val bank_name = map.get("bank_name").get.asInstanceOf[String]
          val address = map.get("address").get.asInstanceOf[String]
          val marriage = map.get("marriage").get.asInstanceOf[String]
          val child_num = map.get("child_num").get.asInstanceOf[String]

          val tableName = TableName.valueOf("user_info")
          val table = connection.getTable(tableName)
          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(sex))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("occupation"), Bytes.toBytes(occupation))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mobile_phone_num"), Bytes.toBytes(mobile_phone_num))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fix_phone_num"), Bytes.toBytes(fix_phone_num))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bank_name"), Bytes.toBytes(bank_name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(address))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("marriage"), Bytes.toBytes(marriage))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("child_num"), Bytes.toBytes(child_num))

          Try(table.put(put)).getOrElse(table.close())//将数据写入HBase，若出错关闭table
          table.close()//分区数据写入HBase后关闭连接
        })
        connection.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
