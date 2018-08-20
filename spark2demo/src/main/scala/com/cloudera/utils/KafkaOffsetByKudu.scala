package com.cloudera.utils

import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import util.control.Breaks._
import scala.collection.JavaConverters._

/**
  * package: com.cloudera.utils
  * describe: 使用Kudu管理Kafka的Offset
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/8/6
  * creat_time: 下午5:46
  * 公众号：Hadoop实操
  */
object KafkaOffsetByKudu {

  /**
    * 用于存储Offset的建表Schema
    */
  val KafkOffset = StructType(
        //         col name   type     nullable?
        StructField("topic", StringType , false) ::
        StructField("group" , StringType, false ) ::
        StructField("partition" , IntegerType, false ) ::
        StructField("offset_id" , LongType, false ) ::
        StructField("timestamp", LongType, false) :: Nil
  )

  /**
    * 定义一个KafkOffset对象
    * @param topic
    * @param group
    * @param partition
    * @param offset_id
    * @param timestamp
    */
  case class KafkOffsetInfo (
    topic: String,
    group: String,
    partition: Int,
    offset_id: Long,
    timestamp: Long
  )

  /**
    * 创建一个用于存放Topic Offset信息的Kudu表
    * @param kuduContext
    * @param tableName
    */
  def init_kudu_tb(kuduContext: KuduContext, tableName: String): Unit = {
    //判断表是否存在
    if(!kuduContext.tableExists(tableName)) {
      println("create Kudu Table :" + tableName)
      val createTableOptions = new CreateTableOptions()
      createTableOptions.addHashPartitions(List("topic","group", "partition").asJava, 8).setNumReplicas(3)
      kuduContext.createTable(tableName, KafkOffset, Seq("topic","group", "partition"), createTableOptions)
    }
  }

  /**
    * 获取最后记录的Kafka Topic Offset
    * @param topics
    * @param group
    * @param tableName
    * @param kuduContext
    * @param spark
    * @return
    */
  def getLastCommittedOffsets(topics : Set[String], group: String, tableName: String, kuduContext: KuduContext, spark:SparkSession, zklist: String) : Map[TopicPartition,Long] = {
    var fromOffsetMap = Map[TopicPartition,Long]()

    //通过Zookeeper获取topics的Partition
    //如果kudu表中存储的数据为空，则需要将所有的partition设置为从0开始
    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zklist, 30000, 3000)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2,false)
    //通过Zookeeper获取相应Topic的Partition及Offset
    val zKNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(topics.toSeq)

    import spark.implicits._
    val rdd_offset = kuduContext.kuduRDD(spark.sparkContext, tableName, Seq("topic","group", "partition" ,"offset_id"))

    //用于缓存需要更新的Offset的数据
    var list = List[KafkOffsetInfo]()

    if(rdd_offset.isEmpty()) { //如果查询的Kudu表数据为空则使用ZK获取到的partition，并将所有的partition的offset设置为0
      topics.foreach(topic => {
        zKNumberOfPartitionsForTopic.get(topic).foreach(_.foreach(partition_id => {
          fromOffsetMap += (new TopicPartition(topic, partition_id) -> 0)
          list.+:(new KafkOffsetInfo(
            topic,
            group,
            partition_id,
            0,
            System.currentTimeMillis()
          ))
        }))
      })
    } else {
      rdd_offset.map(row => {
        val tmp_topic = row.getAs[String]("topic")
        val tmp_group = row.getAs[String]("group")
        val partition_id = row.getAs[Int]("partition")
        breakable{
          if(!topics.contains(tmp_topic) || !group.equals(tmp_group)) break
        }
        zKNumberOfPartitionsForTopic.get(tmp_topic).foreach(_.foreach(tmp_partition_id => {
          if(tmp_partition_id == partition_id) {
            fromOffsetMap += (new TopicPartition(tmp_topic, partition_id) -> row.getAs[Long]("offset_id"))
          } else {
            fromOffsetMap += (new TopicPartition(tmp_topic, partition_id) -> 0)
            //将该对象存入Kudu的KafkaOffset表中
            list.+:(new KafkOffsetInfo(
              tmp_topic,
              tmp_group,
              tmp_partition_id,
              0,
              System.currentTimeMillis()
            ))
          }
        }))
      })
    }

    //将相应Topic的Offset信息更新到kudu表，包含新增的
    if(!list.isEmpty) {
      kuduContext.upsertRows(spark.sqlContext.createDataFrame(list), tableName)
    }

    fromOffsetMap
  }

  /**
    * 将消费的Kafka Offset保存到Kudu表
    * @param kuduContext
    * @param spark
    * @param kafka_rdd
    * @param groupName
    * @param offsetTableName
    */
  def saveOffset(kuduContext: KuduContext, spark:SparkSession, kafka_rdd:RDD[ConsumerRecord[String, String]], groupName:String, offsetTableName:String):Unit = {

    val kafkaOffsetDF = spark.sqlContext.createDataFrame(
      kafka_rdd.map(line => {
        new KafkaOffsetByKudu.KafkOffsetInfo(
          line.topic(),
          groupName,
          line.partition(),
          line.offset(),
          System.currentTimeMillis()
        )
      })
    )
    kuduContext.upsertRows(kafkaOffsetDF, offsetTableName)
  }
}
