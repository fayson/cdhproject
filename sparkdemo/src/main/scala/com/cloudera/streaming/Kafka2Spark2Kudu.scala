package com.cloudera.streaming

import java.io.{File, FileInputStream}

import scala.collection.JavaConversions._
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

import scala.util.parsing.json.JSON

/**
  * package: com.cloudera.streaming
  * describe: SparkStreaming 应用实时读取Kafka数据，解析后存入Kudu
  * 使用spark-submit的方式提交作业
    spark-submit --class com.cloudera.streaming.Kafka2Spark2Kudu \
    --master yarn-client --num-executors 3 --driver-memory 1g \
    --driver-cores 1 --executor-memory 1g --executor-cores 1 \
    spark-demo-1.0-SNAPSHOT.jar
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/5/28
  * creat_time: 上午10:09
  * 公众号：Hadoop实操
  */
object Kafka2Spark2Kudu {
  Logger.getLogger("org").setLevel(Level.ERROR) //设置日志级别

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf/0285.properties"

  /**
    * 建表Schema定义
    */
  val userInfoSchema = StructType(
      //         col name   type     nullable?
      StructField("id", StringType , false) ::
      StructField("name" , StringType, true ) ::
      StructField("sex" , StringType, true ) ::
      StructField("city" , StringType, true ) ::
      StructField("occupation" , StringType, true ) ::
      StructField("tel" , StringType, true ) ::
      StructField("fixPhoneNum" , StringType, true ) ::
      StructField("bankName" , StringType, true ) ::
      StructField("address" , StringType, true ) ::
      StructField("marriage" , StringType, true ) ::
      StructField("childNum", StringType , true ) :: Nil
  )

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
      System.out.println(Kafka2Spark2Kudu.getClass.getClassLoader.getResource("0285.properties"))
      val in = Kafka2Spark2Kudu.getClass.getClassLoader.getResourceAsStream("0285.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }

    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val kuduMaster = properties.getProperty("kudumaster.list")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)

    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics) || StringUtils.isEmpty(kuduMaster)) {
      println("未配置Kafka和KuduMaster信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet

    val sparkConf = new SparkConf().setAppName("Kafka2Spark2Kudu")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //引入隐式
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val kuduContext = new KuduContext(kuduMaster, sc)

    //判断表是否存在
    if(!kuduContext.tableExists("user_info")) {
      println("create Kudu Table :{user_info}")
      val createTableOptions = new CreateTableOptions()
      createTableOptions.addHashPartitions(List("id"), 8).setNumReplicas(3)
      kuduContext.createTable("user_info", userInfoSchema, Seq("id"), createTableOptions)
    }

    dStream.foreachRDD(rdd => {
      //将rdd数据重新封装为Rdd[UserInfo]
      val newrdd = rdd.map(line => {
        val jsonObj =  JSON.parseFull(line._2)
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
      val userinfoDF = sqlContext.createDataFrame(newrdd)
      kuduContext.upsertRows(userinfoDF, "user_info")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
