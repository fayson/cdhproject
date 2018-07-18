package com.cloudera.streaming

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * package: com.cloudera.streaming
  * describe: Kerberos环境中Spark2Streaming应用实时读取Kafka数据，解析后存入HDFS
  * spark2-submit --class com.cloudera.streaming.Kafka2Spark2HDFS \
  * --master yarn \
  * --deploy-mode client \
  * --executor-memory 2g \
  * --executor-cores 2 \
  * --driver-memory 2g \
  * --num-executors 2 \
  * --queue default  \
  * --principal fayson@FAYSON.COM \
  * --keytab /data/disk1/spark2streaming-kafka-hdfs/conf/fayson.keytab \
  * --driver-java-options "-Djava.security.auth.login.config=/data/disk1/spark2streaming-kafka-hdfs/conf/jaas.conf" \
  * --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/data/disk1/spark2streaming-kafka-hdfs/conf/jaas.conf" \
  * spark2-demo-1.0-SNAPSHOT.jar
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/7/17
  * creat_time: 下午11:08
  * 公众号：Hadoop实操
  */
object Kafka2Spark2HDFS {

  Logger.getLogger("com").setLevel(Level.ERROR) //设置日志级别

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf/0292.properties"

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if(!file.exists()) {
      System.out.println(Kafka2Spark2Hive.getClass.getClassLoader.getResource("0292.properties"))
      val in = Kafka2Spark2Hive.getClass.getClassLoader.getResourceAsStream("0292.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }

    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)

    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics)) {
      println("未配置Kafka信息...")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet

    val spark = SparkSession.builder().appName("Kafka2Spark2HDFS-kerberos").config(new SparkConf()).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers
      , "auto.offset.reset" -> "latest"
      , "security.protocol" -> "SASL_PLAINTEXT"
      , "sasl.kerberos.service.name" -> "kafka"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "testgroup"
    )

    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    dStream.foreachRDD(rdd => {

      val newrdd = rdd.map(line => {
        val jsonObj =  JSON.parseFull(line.value())
        val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
        //将Map数据转为以","隔开的字符串
        val userInfoStr = map.get("id").get.asInstanceOf[String].concat(",")
          .concat(map.get("name").get.asInstanceOf[String]).concat(",")
          .concat(map.get("sex").get.asInstanceOf[String]).concat(",")
          .concat(map.get("city").get.asInstanceOf[String]).concat(",")
          .concat(map.get("occupation").get.asInstanceOf[String]).concat(",")
          .concat(map.get("mobile_phone_num").get.asInstanceOf[String]).concat(",")
          .concat(map.get("fix_phone_num").get.asInstanceOf[String]).concat(",")
          .concat(map.get("bank_name").get.asInstanceOf[String]).concat(",")
          .concat(map.get("address").get.asInstanceOf[String]).concat(",")
          .concat(map.get("marriage").get.asInstanceOf[String]).concat(",")
          .concat(map.get("child_num").get.asInstanceOf[String])
        userInfoStr
      })

      //将解析好的数据已流的方式写入HDFS，未使用RDD的方式可以避免数据被覆盖
      newrdd.foreachPartition(partitionrecord => {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        val path =  new Path("/tmp/kafka-data/test.txt")
        //创建一个输出流
        val outputStream = if (fs.exists(path)){
          fs.append(path)
        }else{
          fs.create(path)
        }
        partitionrecord.foreach(line => outputStream.write((line + "\n").getBytes("UTF-8")))
        outputStream.close()
      })

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
