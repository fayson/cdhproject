package com.cloudera;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ApiUtils;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiYarnApplicationAttributeList;
import com.cloudera.api.v1.NameservicesResource;
import com.cloudera.api.v10.ServicesResourceV10;
import com.cloudera.api.v6.YarnApplicationsResource;
import com.cloudera.api.v8.RolesResourceV8;

/**
 * package: com.cloudera
 * describe: 使用Cloudera Manager API实现Yarn动态资源池分配，基于Fair Schedule
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/8/7
 * creat_time: 下午12:04
 * 公众号：Hadoop实操
 */
public class DynamicResourcePoolAllocation {

    private static String cm_host = "cdh01.fayson.com";
    private static int cm_port = 7180;
    private static String cm_username = "admin";
    private static String cm_password = "admin";

    public static void main(String[] args) {
        //使用ClouderaManagerClientBuilder初始化ApiRootResource
        ApiRootResource apiRootResource = new ClouderaManagerClientBuilder()
                .withHost(cm_host)
                .withUsernamePassword(cm_username, cm_password)
                .withPort(cm_port)
                .build();

        ApiClusterList clusters = apiRootResource.getRootV19().getClustersResource().readClusters(DataView.SUMMARY);
        String cluster_name = "";
        for (ApiCluster cluster : clusters) {
            cluster_name = cluster.getName();
        }

        RolesResourceV8 yarnApplicationsResource = apiRootResource.getRootV10().getClustersResource().getServicesResource(cluster_name).getRolesResource("yarn");
        for (ApiRole apiRole : yarnApplicationsResource.readRoles()) {
            apiRole.getConfig().forEach(apiConfig -> {
                System.out.println(apiConfig.getName());
            });
        }

    }
}
