package com.cloudera;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DecimalFormat;
import java.util.Random;

/**
 * package: com.cloudera
 * describe: 随机生成一个Int数字，用与和天时间的时间戳相加获取
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/4/27
 * creat_time: 下午7:09
 * 公众号：Hadoop实操
 */
public class RandomTime extends UDF {


    /**
     * 随机生成一个Int类型数字
     * 86399
     * @return
     */
    public static int evaluate(int max) {
        int result = 0;
        int min = 0;
        result = min + new Random().nextInt(max);
        return result;
    }


    public static void main(String[] args) {
        for (int i =0 ; i < 10; i++)
            System.out.println(evaluate(86399));
    }
}
