package com.cloudera.hdfs

import java.util.Properties

import com.cloudera.utils.{ClientUtils, HDFSUtils}
import org.apache.hadoop.fs.FileSystem

/**
  * package: com.cloudera.hdfs
  * describe: Scala访问Kerberos环境下的HDFS示例
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/11/13
  * creat_time: 下午9:02
  * 公众号：Hadoop实操
  */
object OperatorHDFSByAPI {

  def main(args: Array[String]): Unit = {
    //加载客户端配置参数
    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream("/client.properties"))

    //初始化HDFS Configuration 配置
    val configuration = ClientUtils.initConfiguration()

    //集群启用Kerberos，代码中加入Kerberos环境
    ClientUtils.initKerberosENV(configuration, false, properties)

    val fileSystem = FileSystem.get(configuration)

    val testPath = "/fayson/test"
    //创建HDFS目录
    HDFSUtils.mkdir(fileSystem, testPath)
    //设置目录属主及组
    HDFSUtils.setowner(fileSystem, testPath, "hive", "hive")
    //设置指定HDFS路径的权限
    HDFSUtils.setPermission(fileSystem, testPath, "771")
    //设置指定HDFS目录的ACL
    HDFSUtils.setAcl(fileSystem, testPath)
    //递归指定路径下所有目录及文件
    HDFSUtils.recursiveDir("/user/hive/warehouse/test.db/", fileSystem)

    fileSystem.close()
  }

}
