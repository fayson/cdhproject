package com.cloudera.streaming.nokerberos

import java.io.{File, FileInputStream}
import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.cloudera.utils.KafkaOffsetByKudu
import scala.util.parsing.json.JSON

/**
  * package: com.cloudera.streaming.nokerberos
  * describe: 用于测试使用Kudu管理Kakfa的Offset
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/8/9
  * creat_time: 下午5:50
  * 公众号：Hadoop实操
  */
object KafkaOffsetTest {

  Logger.getLogger("*").setLevel(Level.WARN) //设置日志级别

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf"
  /**
    * 用于存储Kafka消息到Kudu的建表Schema定义
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
    val file = new File(confPath + File.separator + "0295.properties")
    if(!file.exists()) {
      val in = Kafka2Spark2Hbase.getClass.getClassLoader.getResourceAsStream("0295.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(file))
    }

    val offsetTableName = properties.getProperty("offset.tablename")
    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val groupName = properties.getProperty("group.id")
    val kuduMaster = properties.getProperty("kudumaster.list")
    val zklist = properties.getProperty("zookeeper.list")

    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)

    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)

    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics) || StringUtils.isEmpty(groupName)) {
      System.exit(0)
    }

    val topicsSet = topics.split(",").toSet

    val spark = SparkSession.builder()
      .appName("Kafka2Spark2Kudu-Offset")
      .config(new SparkConf())
      .getOrCreate()

    //引入隐式
    import spark.implicits._
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
    KafkaOffsetByKudu.init_kudu_tb(kuduContext, offsetTableName)

    val fromOffsetMap = KafkaOffsetByKudu.getLastCommittedOffsets(topicsSet, groupName, offsetTableName, kuduContext, spark, zklist)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> groupName
    )

    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams, fromOffsetMap))

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

      //数据更新成功后，更新Topic Offset数据到Kudu表中
      KafkaOffsetByKudu.saveOffset(kuduContext, spark, rdd, groupName, offsetTableName)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
