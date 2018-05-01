package com.cloudera.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;

/**
 * package: com.cloudera.hbase
 * describe: 文本文件入库HBase
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/04/30
 * creat_time: 上午12:49
 * 公众号：Hadoop实操
 */
public class Text2HBase {

	// 本地linux磁盘输出路径
	static String inpath = "C:\\Users\\17534\\Desktop\\full-text-index";
	static String outpath = "";
	static SequenceFile.Writer writer = null;
	static HTable htable = null;

	public static void main(String[] args) throws Exception {

		// inpath = args[0];
		// outpath = args[1];
		// String zklist = args[2];

		// HBase入库
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConf.setStrings("hbase.zookeeper.quorum", "ip-172-31-5-38.ap-southeast-1.compute.internal");
		// 指定表名
		htable = new HTable(hbaseConf, "TextHbase");

		File inputFile = new File(inpath);
		File[] files = inputFile.listFiles();
		for (File file : files) {
			String rowKey = file.getName();
			// 指定ROWKEY的值
			Put put = new Put(Bytes.toBytes(rowKey));
			// 指定列簇名称、列修饰符、列值 temp.getBytes()
			put.addColumn("textinfo".getBytes(), "content".getBytes(), getSource(file.getAbsolutePath()));
			htable.put(put);
		}

	}

	public static byte[] getSource(String URL) throws IOException {
		File file = new File(new String(URL.getBytes(), "UTF-8"));
		file.length();
		FileInputStream is = new FileInputStream(file);
		byte[] bytes = new byte[(int) file.length()];
		int len = 0;
		while ((len = is.read(bytes)) != -1) {
			is.read(bytes);
		}
		is.close();
		return bytes;
	}
}
