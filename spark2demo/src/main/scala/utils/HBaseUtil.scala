package utils

import java.io.File
import java.security.PrivilegedAction
import org.apache.hadoop.hbase.{HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

/**
  * package: utils
  * describe: 访问Kerberos环境下的HBase
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/6/25
  * creat_time: 下午10:46
  * 公众号：Hadoop实操
  */
object HBaseUtil {

  /**
    * HBase 配置文件路径
    * @param confPath
    * @return
    */
  def getHBaseConn(confPath: String, principal: String, keytabPath: String): Connection = {
    val configuration = HBaseConfiguration.create
    val coreFile = new File(confPath + File.separator + "core-site.xml")
    if(!coreFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
      configuration.addResource(in)
    }
    val hdfsFile = new File(confPath + File.separator + "hdfs-site.xml")
    if(!hdfsFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hdfs-site.xml")
      configuration.addResource(in)
    }
    val hbaseFile = new File(confPath + File.separator + "hbase-site.xml")
    if(!hbaseFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hbase-site.xml")
      configuration.addResource(in)
    }

    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Connection] {
      override def run(): Connection = ConnectionFactory.createConnection(configuration)
    })
  }


}
