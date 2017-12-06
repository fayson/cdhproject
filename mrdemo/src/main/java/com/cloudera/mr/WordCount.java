package com.cloudera.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * package: com.cloudera.mr
 * describe: 打包jar到集群使用hadoop命令提交作业示例
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/6
 * creat_time: 下午11:30
 * 公众号：Hadoop实操
 */
public class WordCount {

    private static Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {

        logger.info(args[0] + "-----" + args[1]);
        try {
            Configuration conf = new Configuration();
            Job wcjob = Job.getInstance(conf);
            wcjob.setJobName("MyWordCount");
            wcjob.setJarByClass(WordCount.class);

            wcjob.setJarByClass(InitMapReduceJob.class);

            wcjob.setMapperClass(WordCountMapper.class);
            wcjob.setReducerClass(WordCountReducer.class);
            wcjob.setMapOutputKeyClass(Text.class);
            wcjob.setMapOutputValueClass(LongWritable.class);
            wcjob.setOutputKeyClass(Text.class);
            wcjob.setOutputValueClass(LongWritable.class);
            FileInputFormat.setInputPaths(wcjob, args[0]);

            FileOutputFormat.setOutputPath(wcjob, new Path(args[1]));
            //调用job对象的waitForCompletion()方法，提交作业。
            boolean res = wcjob.waitForCompletion(true);
            System.exit(res ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
