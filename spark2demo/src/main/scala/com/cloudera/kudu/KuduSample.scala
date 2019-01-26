package com.cloudera.kudu

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

/**
  * package: com.cloudera.kudu
  * describe: Spark2 使用KuduContext访问Kudu
  * 该示例业务逻辑，Spark读取Hive的ods_user表前10条数据，写入Kudu表（通过ods_user表的Schema创建kudu表）
  * 读取kudu_user_info表数据，将返回的rdd转换为DataFrame写入到Hive的kudu2hive表中
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2019/1/25
  * creat_time: 上午10:58
  * 公众号：Hadoop实操
  */
object KuduSample {

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
    address: String
  )

  def main(args: Array[String]): Unit = {

    val kuduMaster = "cdh1.fayson.com,cdh2.fayson.com,cdh3.fayson.com"
    val kuduTableName = "kudu_user_info"
    val hiveTableName = "kudu2hive"

    //Spark Conf配置信息
    val conf = new SparkConf()
      .setAppName("Spark2OnKuduSample")
      .set("spark.master", "yarn")
      .set("spark.submit.deployMode", "client")

    //初始化SparkSession对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //引入隐式
    import spark.implicits._
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    //查询出Hive表数据
    val odsuserdf = spark.sql("select * from ods_user limit 10")

    //判断表是否存在
    if(!kuduContext.tableExists(kuduTableName)) {
      val createTableOptions = new CreateTableOptions()
      createTableOptions.addHashPartitions(List("id").asJava, 8).setNumReplicas(3)
      kuduContext.createTable(kuduTableName, odsuserdf.schema.add("id", StringType, false), Seq("id"), createTableOptions)
    }
    //将Hive中ods_user表的前10条数据写入到kudutableName表中
    kuduContext.upsertRows(odsuserdf, kuduTableName)

    //读取出kuduTableName表的数据
    val kudurdd = kuduContext.kuduRDD(spark.sparkContext, kuduTableName, Seq("id","name","sex","city","occupation","tel","fixPhoneNum","bankName","address"))

    //将kudurdd转换转换为DataFrame对象，写到hive的表中
    spark.sqlContext.createDataFrame(kudurdd.mapPartitions(partition => {
      partition.map(row =>{new UserInfo(
        row.getAs[String](0),
        row.getAs[String](1),
        row.getAs[String](2),
        row.getAs[String](3),
        row.getAs[String](4),
        row.getAs[String](5),
        row.getAs[String](6),
        row.getAs[String](7),
        row.getAs[String](8)
      )})
    })).write.saveAsTable(hiveTableName)

    spark.close()
  }

}
