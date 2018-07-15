package com.cloudera.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * package: com.cloudera.jdbc
  * describe: TODO
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/6/27
  * creat_time: 下午4:14
  * 公众号：Hadoop实操
  */
object ImpalaJDBC {

  def read(sqlContext: SparkSession, props: Properties): DataFrame = {

    //将加载的properties对象转换为Scala中的MAP对象
    import scala.collection.JavaConverters._
    val map = props.asScala
    sqlContext.read.format("jdbc").options(map).load()
  }

  def read1(props: Properties): Unit = {

    //将加载的properties对象转换为Scala中的MAP对象
    import scala.collection.JavaConverters._
    val map = props.asScala
    map.foreach(t => {
      println(t._1)
    })
  }

  def test(name: String): String = {

    "Hello " + name
  }
}