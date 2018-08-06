package com.cloudera.streaming.nokerberos

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

import scala.util.parsing.json.JSON
import scala.collection.JavaConverters._

/**
  * package: com.cloudera.streaming.nokerberos
  * 使用spark2-submit的方式提交作业
    spark2-submit --class com.cloudera.streaming.nokerberos.Kafka2Spark2Kudu \
    --master yarn \
    --deploy-mode client \
    --executor-memory 2g \
    --executor-cores 2 \
    --driver-memory 2g \
    --num-executors 2 \
    spark2-demo-1.0-SNAPSHOT.jar
  * creat_user: Fayson
  * email: htechinfo@163.com
  * creat_date: 2018/8/6
  * creat_time: 下午5:05
  * 公众号：Hadoop实操
  */
object Kafka2Spark2Kudu {

  Logger.getLogger("com").setLevel(Level.ERROR) //设置日志级别

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf/0294.properties"

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
      System.out.println(Kafka2Spark2Kudu.getClass.getClassLoader.getResource("0294.properties"))
      val in = Kafka2Spark2Kudu.getClass.getClassLoader.getResourceAsStream("0294.properties")
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

    val spark = SparkSession.builder().appName("Kafka2Spark2Kudu-nokerberos").config(new SparkConf()).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers
      , "auto.offset.reset" -> "latest"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> properties.getProperty("group.id")
    )

    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //引入隐式
    import spark.implicits._
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    //判断表是否存在
    if(!kuduContext.tableExists("user_info")) {
      println("create Kudu Table :{user_info}")
      val createTableOptions = new CreateTableOptions()
      createTableOptions.addHashPartitions(List("id").asJava, 8).setNumReplicas(3)
      kuduContext.createTable("user_info", userInfoSchema, Seq("id"), createTableOptions)
    }

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
      kuduContext.upsertRows(userinfoDF, "user_info")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
