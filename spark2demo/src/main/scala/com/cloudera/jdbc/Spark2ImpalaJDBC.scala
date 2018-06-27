package com.cloudera.jdbc

import java.io.{File, FileInputStream}
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * package: com.cloudera.jdbc
  * describe: Spark2使用JDBC方式访问Kerberos环境下的Impala
  * 该示例使用到的配置文件有0290-jdbc.properties和jaas-impala.conf
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/6/27
  * creat_time: 下午4:25
  * 公众号：Hadoop实操
  */
object Spark2ImpalaJDBC {

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf"

  def main(args: Array[String]): Unit = {

    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath + File.separator + "0290-jdbc.properties")
    if(!file.exists()) {
      val in = Spark2ImpalaJDBC.getClass.getClassLoader.getResourceAsStream("0290-jdbc.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(file))
    }

    //将加载的properties对象转换为Scala中的MAP对象
    import scala.collection.JavaConverters._
    val map = properties.asScala

    val sparkConf = new SparkConf()
//    sparkConf.set("spark.executor.extraJavaOptions","-Djava.security.auth.login.config=/data/disk1/spark-jdbc-impala/conf/jaas-impala.conf -Djavax.security.auth.useSubjectCredsOnly=false")

    //初始化SparkContext
    val spark = SparkSession
      .builder().config(sparkConf)
      .appName("Spark2-JDBC-Impala-Kerberos")
      .getOrCreate()

    //通过jdbc访问Impala获取一个DataFrame
    val dataframe = spark.read.format("jdbc").options(map).load()
    dataframe.show(10)
  }
}
