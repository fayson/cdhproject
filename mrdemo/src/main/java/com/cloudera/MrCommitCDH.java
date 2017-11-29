package com.cloudera;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dinfo.app.utils.flow.DistributeComInputParam;
import com.dinfo.bigdata.util.LoginUtil;
import com.dinfo.bigdata.util.RemoteHadoopUtil;

public class MrCommitCDH implements MRInter {
	
	@Override
	public Map<String, String> excute(String[] args) {
		String inputPath=null;
		String outPath = null;
		String user=null;
		String keytab=null;
		try {
			if(args.length<1) {
				args = new String[] {"/dinfo/input/testMR.txt", "/dinfo/output" + new Date().getTime(),"dinfo","dinfo.keytab"};
			}
			 inputPath=args[0];
			 outPath = args[1];
			 user=args[2];
			 keytab=args[3];
			System.out.println("inputPath:"+inputPath);
			System.out.println("outPath:"+outPath);
			System.out.println("user:"+user);
			System.out.println("keytab:"+keytab);
			System.out.println("=============MrCommit==================Start==========");
			String confPath = System.getProperty("user.dir")+File.separator+"conf";
			System.out.println("confPath:"+confPath);
			 RemoteHadoopUtil.setConf(MrCommitCDH.class, Thread.currentThread(),
					 confPath);
			 Configuration conf = new Configuration();
			 LoginUtil.login(user,keytab,conf);
//			Configuration conf = LoginUtil.login(user,keytab);
			conf.set("mapred.jar","/dinfo/mrjar.jar");
			Job job = Job.getInstance(conf);
			job.setJobName("test5");
			job.setMaxMapAttempts(10);
			
			job.setNumReduceTasks(1); // 设置reduce数.
//			DistributedCache.addFileToClassPath(new Path("hdfs://192.168.191.112:9000/lib/dinfo-common-1.1.3.jar"),
//					job.getConfiguration());
			
			// 配置作业各个类
			job.setJarByClass(MrCommitCDH.class);
			job.setMapperClass(BeanMapCDH.class);
//			job.setCombinerClass(BeanReduceHuaWei.class);
			job.setReducerClass(BeanReduceCDH.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			int ret = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("=============MrCommit==================END==========");
		return null;
	}
	
	public static void main(String[] args) {
		MrCommitCDH mc = new MrCommitCDH();
		mc.excute(args);
	}
	
}
