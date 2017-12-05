package com.cloudera.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * package: com.cloudera.mr
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/4
 * creat_time: 下午11:53
 * 公众号：Hadoop实操
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,Context context)
            throws IOException, InterruptedException {

        //定义一个累加计数器 定义为Long类型
        long count = 0;
        for(LongWritable value:values){
            //调用value的get（）方法将long值取出来
            count += value.get();
        }
        //输出<单词：count>键值对
        context.write(key, new LongWritable(count));
    }
}