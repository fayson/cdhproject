//package com.cloudera.solr;
//
//import org.apache.http.HttpEntity;
//import org.apache.http.HttpResponse;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.Credentials;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.client.methods.HttpUriRequest;
//import org.apache.http.client.params.AuthPolicy;
//import org.apache.http.impl.auth.SPNegoSchemeFactory;
//import org.apache.http.impl.client.DefaultHttpClient;
//import sun.security.acl.PrincipalImpl;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.net.Authenticator;
//import java.net.PasswordAuthentication;
//import java.net.URL;
//import java.security.Principal;
//
///**
// * package: com.cloudera.solr
// * describe: TODO
// * creat_user: Fayson
// * email: htechinfo@163.com
// * creat_date: 2017/11/26
// * creat_time: 上午1:52
// * 公众号：Hadoop实操
// */
//public class Test {
//    public static void main(String[] args) {
//        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
//        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/login.conf");
//        System.setProperty("sun.security.krb5.debug", "true");
//        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
//
//        try {
//            Authenticator.setDefault(new MyAuthenticator());
//            URL url = new URL("http://ip-172-31-26-80.ap-southeast-1.compute.internal:8983/solr/collection1");
//            InputStream ins = url.openConnection().getInputStream();
//            BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
//            String str;
//            while((str = reader.readLine()) != null) {
//                System.out.println(str);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
////        DefaultHttpClient httpclient = new DefaultHttpClient();
////
////        httpclient.getAuthSchemes().register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory());
////
////        Credentials use_jaas_creds = new Credentials() {
////            public String getPassword() {
////                return "123456";
////            }
////            public Principal getUserPrincipal() {
////                Principal principal = new PrincipalImpl("fayson@CLOUDERA.COM");
////                return principal;
////            }
////        };
////
////        httpclient.getCredentialsProvider().setCredentials(new AuthScope("ip-172-31-26-80.ap-southeast-1.compute.internal", -1, null), use_jaas_creds);
////
////        HttpUriRequest request = new HttpGet("http://ip-172-31-26-80.ap-southeast-1.compute.internal:8983/solr/");
////        HttpResponse response = null;
////        try {
////            response = httpclient.execute(request);
////            HttpEntity entity = response.getEntity();
////
////            System.out.println("----------------------------------------");
////            System.out.println(response.getStatusLine());
////            System.out.println("----------------------------------------");
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//
//    }
//
//    static final String kuser = "fayson"; // your account name
//    static final String kpass = "123456"; // your password for the account
//
//    static class MyAuthenticator extends Authenticator {
//        public PasswordAuthentication getPasswordAuthentication() {
//            // I haven't checked getRequestingScheme() here, since for NTLM
//            // and Negotiate, the usrname and password are all the same.
//            System.err.println("Feeding username and password for " + getRequestingScheme());
//            return (new PasswordAuthentication(kuser, kpass.toCharArray()));
//        }
//    }
//}
