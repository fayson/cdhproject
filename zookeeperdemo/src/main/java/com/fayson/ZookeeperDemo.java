package com.fayson;

import com.sun.security.auth.callback.TextCallbackHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ZookeeperDemo {

    public static LoginContext lc = null;

    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "/Users/taopanlong/Documents/develop/kerberos/local/krb5_204.conf");
        System.setProperty("java.security.auth.login.config", "/Users/taopanlong/Documents/develop/work/cdhproject/zookeeperdemo/src/main/resources/jaas.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");

        try {
            String hostPort = "cdh01.hadoop.com:2181,cdh02.hadoop.com:2181,cdh03.hadoop.com:2181";
            String zpath = "/";
            List<String> zooChildren = new ArrayList<String>();
            ZooKeeper zk = new ZooKeeper(hostPort, 2000, null);
            if (zk != null) {
                try {

                    zk.create("/tmptest", "tmptest".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
//                    zk.delete("/tmptest", -1);

                    zooChildren = zk.getChildren(zpath, false);
                    List<ACL> list = new ArrayList<>();

                    System.out.println("Znodes of '/': ");

                    for (String child : zooChildren) {
                        //print the children
                        System.out.println(child);
                    }
                    zk.delete("/tmptest", -1);

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void login() {

        try {
            lc = new LoginContext("Client", new TextCallbackHandler());
            lc.login();
            System.out.println(lc.getSubject().getPrincipals().size());
        } catch (LoginException e) {
            e.printStackTrace();
        }
    }

    public static void relogin() {
        try {
            lc.logout();
            lc = new LoginContext("Client", new TextCallbackHandler());
            lc.login();
        } catch (LoginException e) {
            e.printStackTrace();
        }

    }
}