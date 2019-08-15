package com.cloudera;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudera.utils.HttpUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * package: com.fayson
 * describe: 获取CDSW每个Session的日志输出
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2019/7/6
 * creat_time: 11:13 PM
 * 公众号：Hadoop实操
 */
public class ScanLiveLog {

    private static final String dbPath = "/Users/fayson/Documents/develop/rocksdb/livelog";

    static {
        RocksDB.loadLibrary();
    }

    public static RocksDB rocksDB;

    public static void main(String[] args) {
        String sessionUrl = "http://master.hadoop.com/api/v1/site/dashboards?limit=30&offset=0&order_by=created_at&order_sort=desc&start_max=2019-07-06T22:54:06%2B08:00&start_min=2019-06-29T22:54:06%2B08:00";

        // Load rocksdb
        loadRocksDB(dbPath);
        try {
            //根据时间范围获取所有的Session
            String result = HttpUtils.getAccess(sessionUrl, "admin", "admin");
            JSONArray jsonArray = JSONArray.parseArray(result);
            jsonArray.forEach(action -> {
                JSONObject jsonObject = (JSONObject)action;
                String containerID = jsonObject.getString("id");

                Map<Integer, String> map = getContent(containerID);
                System.out.println(containerID + "------------" + map.size());

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 加载RocksDB数据目录
     * @param dbPath
     */
    public static void loadRocksDB(String dbPath) {
        Options options = new Options();
        options.setCreateIfMissing(true);

        // 文件不存在，则先创建文件
        if (!Files.isSymbolicLink(Paths.get(dbPath))){
            System.out.println("CDSW livelog data director not exists");
        }
        try {
            rocksDB = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据ContainerID获取livelog中所有的output信息
     * @param containerID
     * @return
     */
    public static Map<Integer, String> getContent(String containerID) {
        Map<Integer, String> map = new HashMap<>();
        int i = 0;
        String index = "\0\0\0\0\0\0\0\0\2";
        while(true) {
            String keyStr = containerID + "_output" +  index.replace((char)2, (char)i);
            String value = getValue(keyStr);
            System.out.println("key:" + keyStr + "," + "Value:" + value);
            if(value != null) {
                map.put(i, value);
                i++;
            } else {
                break;
            }
        }

        return map;
    }

    /**
     * 根据指定的Rowkey获取单条输出的日志信息
     * @param keyStr
     * @return
     */
    public static String getValue(String keyStr) {
        String result = null;
        byte[] key = keyStr.getBytes();
        try {
            byte[] getValue = rocksDB.get(key);
            if(getValue != null && getValue.length > 0) {
                result = new String(getValue);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return result;
    }


    //  RocksDB.DEFAULT_COLUMN_FAMILY
    public static void testDefaultColumnFamily() throws RocksDBException, IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        /**
         *  打印全部[key - value]
         */
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }
    }
}
