package com.cloudera.utils;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

public class LoginUtil {
	
	private static String confPath = System.getProperty("user.dir")+File.separator+"conf";
	private static String user="dinfo";
	private static String keyPath = confPath+File.separator+"dinfo.keytab";
	
	
	public static Configuration login(String user2, String keytab, Configuration configuration) {
		System.out.println("user:"+user2);
		System.out.println("keytab:"+keytab);
		keyPath = confPath+File.separator+keytab;
		Configuration conf =null;
		try {
			System.setProperty("java.security.krb5.conf", confPath+File.separator+"krb5.conf");
		    UserGroupInformation.setConfiguration(configuration);
			configuration.setBoolean("dfs.support.append", true);
			configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
			UserGroupInformation.loginUserFromKeytab(user2, keyPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conf;

	}
	
	
	public static Configuration getHadoopConf(){
		Configuration configuration = new Configuration();
		configuration.addResource(new Path(confPath+File.separator + "core-site.xml"));
		configuration.addResource(new Path(confPath+File.separator + "hdfs-site.xml"));
		configuration.addResource(new Path(confPath+File.separator + "hbase-site.xml"));
		configuration.addResource(new Path(confPath+File.separator + "mapred-site.xml"));
		configuration.addResource(new Path(confPath+File.separator + "yarn-site.xml"));
		configuration.setBoolean("dfs.support.append", true);
		configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
		return configuration;
	}

}
