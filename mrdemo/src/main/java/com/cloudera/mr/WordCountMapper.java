package com.cloudera.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * package: com.cloudera.mr
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/4
 * creat_time: 下午11:52
 * 公众号：Hadoop实操
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     map（）方法实现思路：
     1.获取文件的每行内容
     2.将这行内容切分，调用StringUtils的方法是split方法，分割符为“”，切分后的字符放到字符串数组内
     3.遍历输出<word,1>
     */
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        //获取到一行文件的内容
        String line = value.toString();
        //切分这一行的内容为一个单词数组
        String[] words = StringUtils.split(line, " ");
        //遍历  输出  <word,1>
        for(String word:words){
            context.write(new Text(word), new LongWritable(1));

        }
    }
}
