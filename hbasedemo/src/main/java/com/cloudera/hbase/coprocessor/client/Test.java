package com.cloudera.hbase.coprocessor.client;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * package: com.cloudera.hbase.coprocessor.client
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/14
 * creat_time: 下午6:58
 * 公众号：Hadoop实操
 */
public class Test {
    public static void main(String[] args) {
        Double min = null;
        Double temp = new Double(10.0);
        min = min != null && (temp == null || compare(temp, min) >= 0) ? min : temp;
        temp = new Double(2.0);
        min = min != null && (temp == null || compare(temp, min) >= 0) ? min : temp;


        System.out.println(min.doubleValue());

    }

    public static int compare(Double l1, Double l2) {
        if (l1 == null ^ l2 == null) {
            return l1 == null ? -1 : 1; // either of one is null.
        } else if (l1 == null)
            return 0; // both are null
        return l1.compareTo(l2); // natural ordering.
    }
}
