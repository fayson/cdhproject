package com.cloudera.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;
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

        JSONObject jo = JSONObject.parseObject(jsonStr);
        for(Map.Entry entry:jo.entrySet()){
            resultMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return resultMap;
    }
}