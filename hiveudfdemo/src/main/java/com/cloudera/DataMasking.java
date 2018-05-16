package com.cloudera;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * package: com.cloudera
 * describe: 根据传入的正则表达式和字符串将符合条件的字符串数据屏蔽
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/3/16
 * creat_time: 上午9:39
 * 公众号：Hadoop实操
 */
public class DataMasking extends UDF {

    /**
     * 数据脱敏处理方法
     * @param regx  正则表达式
     * @param replaceStr  将敏感数据替换为指定字符串
     * @param data  待脱敏数据
     * @return  result 脱敏后数据
     */
    public static String evaluate(String regx, String replaceStr, String data) {
        String result = data;
        result = data.replaceAll(regx, replaceStr);
        return result;
    }


    public static void main(String[] args) {
        String regx = "(\\d{3})\\d{4}(\\d{4})";
        String replaceStr = "$1****$2";
        String data = "18664525814";
        System.out.println(evaluate(regx, replaceStr, data));
    }

}
