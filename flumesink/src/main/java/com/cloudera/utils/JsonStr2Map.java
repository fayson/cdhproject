package com.cloudera.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * package: com.cloudera.utils
 * describe: Json字符串转Map
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/6/2
 * creat_time: 下午11:19
 * 公众号：Hadoop实操
 */
public class JsonStr2Map {

    /**
     * 将Json字符串转为Map对象
     * @param jsonStr
     * @return
     */
    public static Map<String, String> jsonStr2Map(String jsonStr) {
        Map<String, String> resultMap = new HashMap<>();
        Pattern pattern = Pattern.compile("(\"\\w+\"):(\"[^\"]+\")");
        Matcher m = pattern.matcher(jsonStr);
        String[] strs = null;
        while (m.find()) {
            strs = m.group().split(":");
            if(strs != null && strs.length == 2) {
                resultMap.put(strs[0].replaceAll("\"", "").trim(), strs[1].trim().replaceAll("\"", ""));
            }
        }
        return resultMap;
    }
}
