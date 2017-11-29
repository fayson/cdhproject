package com.cloudera;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeanAndFile.java
		LoginUtil.java
		RemoteHadoopUtil.javaBeanReduceCDH extends Reducer<Text,IntWritable,Text, IntWritable> {
	private static final Logger logger = LoggerFactory.getLogger(BeanReduceCDH.class);
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
//		context.write(key, values);
		int sum = 0;
		for(IntWritable val:values) {
			sum+=val.get();
		}
		context.write(new Text("\r\n"+key), new IntWritable(sum));
	}
}