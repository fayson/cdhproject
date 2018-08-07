package com.cloudera;

import com.cloudera.utils.HttpUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.HashMap;

/**
 * package: com.cloudera
 * describe: 通过CM提供的API配置Yarn动态资源池
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/8/7
 * creat_time: 下午3:14
 * 公众号：Hadoop实操
 */
public class RestApiConfPool {

    private static String REQ_CLUSTER_URL = "http://cdh01.fayson.com:7180/api/v19/clusters";
    private static String REQ_SETPOOL_URL = "http://cdh01.fayson.com:7180/api/v19/clusters/cluster/services/yarn/config";
    private static String REQ_FRESHPOOL_URL = "http://cdh01.fayson.com:7180/api/v19/clusters/cluster/commands/poolsRefresh";
    private static String USERNAME = "admin";
    private static String PASSWORD = "admin";

    public static void main(String[] args) {

        HashMap<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");

        //获取CM管理的Cluster集群名称
        String result = HttpUtils.getAccessByAuth(REQ_CLUSTER_URL, headers, USERNAME, PASSWORD);
        System.out.println("获取集群信息：" + result);

        //定义资源池配置的JSON配置
        JSONObject requestJson = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        JSONObject yarnjson = new JSONObject();
        String pool_conf = "{\\\"defaultFairSharePreemptionThreshold\\\":null,\\\"defaultFairSharePreemptionTimeout\\\":null,\\\"defaultMinSharePreemptionTimeout\\\":null,\\\"defaultQueueSchedulingPolicy\\\":\\\"fair\\\",\\\"queueMaxAMShareDefault\\\":null,\\\"queueMaxAppsDefault\\\":null,\\\"queuePlacementRules\\\":[{\\\"create\\\":true,\\\"name\\\":\\\"specified\\\",\\\"queue\\\":null,\\\"rules\\\":null},{\\\"create\\\":null,\\\"name\\\":\\\"nestedUserQueue\\\",\\\"queue\\\":null,\\\"rules\\\":[{\\\"create\\\":true,\\\"name\\\":\\\"default\\\",\\\"queue\\\":\\\"users\\\",\\\"rules\\\":null}]},{\\\"create\\\":null,\\\"name\\\":\\\"default\\\",\\\"queue\\\":null,\\\"rules\\\":null}],\\\"queues\\\":[{\\\"aclAdministerApps\\\":\\\"*\\\",\\\"aclSubmitApps\\\":\\\"*\\\",\\\"allowPreemptionFrom\\\":null,\\\"fairSharePreemptionThreshold\\\":null,\\\"fairSharePreemptionTimeout\\\":null,\\\"minSharePreemptionTimeout\\\":null,\\\"name\\\":\\\"root\\\",\\\"queues\\\":[{\\\"aclAdministerApps\\\":null,\\\"aclSubmitApps\\\":null,\\\"allowPreemptionFrom\\\":null,\\\"fairSharePreemptionThreshold\\\":null,\\\"fairSharePreemptionTimeout\\\":null,\\\"minSharePreemptionTimeout\\\":null,\\\"name\\\":\\\"default\\\",\\\"queues\\\":[],\\\"schedulablePropertiesList\\\":[{\\\"impalaDefaultQueryMemLimit\\\":null,\\\"impalaDefaultQueryOptions\\\":null,\\\"impalaMaxMemory\\\":null,\\\"impalaMaxQueuedQueries\\\":null,\\\"impalaMaxRunningQueries\\\":null,\\\"impalaQueueTimeout\\\":null,\\\"maxAMShare\\\":null,\\\"maxChildResources\\\":null,\\\"maxResources\\\":null,\\\"maxRunningApps\\\":null,\\\"minResources\\\":null,\\\"scheduleName\\\":\\\"default\\\",\\\"weight\\\":1.0}],\\\"schedulingPolicy\\\":\\\"drf\\\",\\\"type\\\":null},{\\\"aclAdministerApps\\\":null,\\\"aclSubmitApps\\\":null,\\\"allowPreemptionFrom\\\":null,\\\"fairSharePreemptionThreshold\\\":null,\\\"fairSharePreemptionTimeout\\\":null,\\\"minSharePreemptionTimeout\\\":null,\\\"name\\\":\\\"users\\\",\\\"queues\\\":[],\\\"schedulablePropertiesList\\\":[{\\\"impalaDefaultQueryMemLimit\\\":null,\\\"impalaDefaultQueryOptions\\\":null,\\\"impalaMaxMemory\\\":null,\\\"impalaMaxQueuedQueries\\\":null,\\\"impalaMaxRunningQueries\\\":null,\\\"impalaQueueTimeout\\\":null,\\\"maxAMShare\\\":null,\\\"maxChildResources\\\":null,\\\"maxResources\\\":null,\\\"maxRunningApps\\\":null,\\\"minResources\\\":null,\\\"scheduleName\\\":\\\"default\\\",\\\"weight\\\":2.0}],\\\"schedulingPolicy\\\":\\\"drf\\\",\\\"type\\\":\\\"parent\\\"}],\\\"schedulablePropertiesList\\\":[{\\\"impalaDefaultQueryMemLimit\\\":null,\\\"impalaDefaultQueryOptions\\\":null,\\\"impalaMaxMemory\\\":null,\\\"impalaMaxQueuedQueries\\\":null,\\\"impalaMaxRunningQueries\\\":null,\\\"impalaQueueTimeout\\\":null,\\\"maxAMShare\\\":null,\\\"maxChildResources\\\":null,\\\"maxResources\\\":null,\\\"maxRunningApps\\\":null,\\\"minResources\\\":null,\\\"scheduleName\\\":\\\"default\\\",\\\"weight\\\":1.0}],\\\"schedulingPolicy\\\":\\\"drf\\\",\\\"type\\\":null}],\\\"userMaxAppsDefault\\\":null,\\\"users\\\":[]}";
        yarnjson.put("name", "yarn_fs_scheduled_allocations");
        yarnjson.put("value", pool_conf);
        jsonArray.add(yarnjson);
        requestJson.put("items", jsonArray);
        //注意使用PUT提交，否则会请求失败
        result = HttpUtils.putAccessByAuth(REQ_SETPOOL_URL, headers, StringEscapeUtils.unescapeJava(requestJson.toString()), USERNAME, PASSWORD);

        System.out.println("动态设置Yarn资源池："+ result);

        //刷新资源池
        result = HttpUtils.postAccessByAuth(REQ_FRESHPOOL_URL, headers, null, USERNAME, PASSWORD);

        System.out.println("刷新资源池配置：" + result);
    }
}
