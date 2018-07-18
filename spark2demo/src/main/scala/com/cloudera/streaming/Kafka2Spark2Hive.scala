package com.cloudera.streaming

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON

/**
  * package: com.cloudera.streaming
  * describe: Kerberos环境中Spark2Streaming应用实时读取Kafka数据，解析后存入Hive
  * 使用spark2-submit的方式提交作业
  * spark2-submit --class com.cloudera.streaming.Kafka2Spark2Hive \
   --master yarn \
   --deploy-mode client \
   --executor-memory 2g \
   --executor-cores 2 \
   --driver-memory 2g \
   --num-executors 2 \
   --queue default  \
   --principal hive/admin@FAYSON.COM \
   --keytab /data/disk1/spark2streaming-kafka-hive/conf/hive.keytab \
   --files "/data/disk1/spark2streaming-kafka-hive/conf/jaas.conf#jaas.conf" \
   --driver-java-options "-Djava.security.auth.login.config=/data/disk1/spark2streaming-kafka-hive/conf/jaas.conf" \
   --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/data/disk1/spark2streaming-kafka-hive/conf/jaas.conf" \
   spark2-demo-1.0-SNAPSHOT.jar
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/7/15
  * creat_time: 下午4:01
  * 公众号：Hadoop实操
  */
object Kafka2Spark2Hive {

  Logger.getLogger("com").setLevel(Level.ERROR) //设置日志级别

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf/0291.properties"

  /**
    * 定义一个UserInfo对象
    */
  case class UserInfo (
                        id: String,
                        name: String,
                        sex: String,
                        city: String,
                        occupation: String,
                        tel: String,
                        fixPhoneNum: String,
                        bankName: String,
                        address: String,
                        marriage: String,
                        childNum: String
                      )

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if(!file.exists()) {
      System.out.println(Kafka2Spark2Hive.getClass.getClassLoader.getResource("0291.properties"))
      val in = Kafka2Spark2Hive.getClass.getClassLoader.getResourceAsStream("0291.properties")
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

    val spark = SparkSession.builder().appName("Kafka2Spark2Hive-kerberos").config(new SparkConf()).getOrCreate()
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

    //引入隐式
    import spark.implicits._

    dStream.foreachRDD(rdd => {
      //将rdd数据重新封装为Rdd[UserInfo]
      val newrdd = rdd.map(line => {
        val jsonObj =  JSON.parseFull(line.value())
        val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
        new UserInfo(
          map.get("id").get.asInstanceOf[String],
          map.get("name").get.asInstanceOf[String],
          map.get("sex").get.asInstanceOf[String],
          map.get("city").get.asInstanceOf[String],
          map.get("occupation").get.asInstanceOf[String],
          map.get("mobile_phone_num").get.asInstanceOf[String],
          map.get("fix_phone_num").get.asInstanceOf[String],
          map.get("bank_name").get.asInstanceOf[String],
          map.get("address").get.asInstanceOf[String],
          map.get("marriage").get.asInstanceOf[String],
          map.get("child_num").get.asInstanceOf[String]
        )
      })
      //将RDD转换为DataFrame
      val userinfoDF = spark.sqlContext.createDataFrame(newrdd)

      userinfoDF.write.mode(SaveMode.Append).saveAsTable("ods_user")
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
