package com.cloudera;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BeanMapCDH extends Mapper<Object, Text,Text,IntWritable> {
	private static final Logger logger = LoggerFactory.getLogger(BeanMapCDH.class);
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("value:"+value.toString());
		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);
		while(token.hasMoreTokens()) {
			word.set(token.nextToken());
			context.write(word, one);
//			context.write(one, word);
		}
		
	}
}
