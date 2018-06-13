package com.cloudera.kerberos;

import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 在Kafka集成Sentry后向Kafka生产数据方式，必须指定group.id
 * package: com.cloudera.nokerberos
 * describe: 读取本地用户数据文件，接数据文件解析组装成JSON格式数据发送至指定的Topic
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/28
 * creat_time: 上午10:37
 * 公众号：Hadoop实操
 */
public class ReadUserInfoFileToKafka_0286 {

    public static String confPath = System.getProperty("user.dir") + File.separator + "conf";
    public static Logger logger = LoggerFactory.getLogger(ReadUserInfoFileToKafka_0286.class);

    public static void main(String[] args) {
        if(args.length < 1) {
            System.err.println("缺少输入参数，请指定要处理的text文件");
            System.exit(1);
        }

        //初始化Kerberos信息
        String krb5conf = confPath + File.separator + "krb5.conf";
        if(!new File(krb5conf).exists()) {
            krb5conf = ReadUserInfoFileToKafka_0286.class.getClassLoader().getResource("krb5.conf").getPath();
        }
        String jaasconf = confPath + File.separator + "jaas.conf";
        if(!new File(jaasconf).exists()) {
            krb5conf = ReadUserInfoFileToKafka_0286.class.getClassLoader().getResource("jaas.conf").getPath();
        }

        Properties appProperties = getPublicProps();
        String brokerlist = appProperties.getProperty("bootstrap.servers");
        String topic_name = appProperties.getProperty("topic.name");
        String group_id = appProperties.getProperty("group.id");


        System.setProperty("java.security.krb5.conf", krb5conf);
        System.setProperty("java.security.auth.login.config", jaasconf);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", appProperties.getProperty("krb5.debug", "false")); //Kerberos Debug模式

        String filePath = args[0];
        BufferedReader reader = null;

        try {
            Properties props = getKafkaProps();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
            props.put("group.id", group_id);
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.kerberos.service.name", "kafka");

            Producer<String, String> producer = new KafkaProducer<String, String>(props);

            reader = new BufferedReader(new FileReader(filePath));
            String tempString = null;
            int line = 0;
            System.out.println("start read local file.....");
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                String detailJson = parse_person_info_JSON(tempString);
                ProducerRecord record = new ProducerRecord<String, String>(topic_name, detailJson);
                producer.send(record);
                line++;
            }
            System.out.println("send " + line + " message to " + topic_name);
            reader.close();
            producer.flush();
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }

        }
    }

    /**
     * 将txt文件中的每行数据解析并组装为json字符串
     * @param tempString
     * @return
     */
    private static String parse_person_info_JSON(String tempString) {
        if(tempString != null && tempString.length() > 0) {
            Map<String, String> resultMap = null;
            String[] detail = tempString.split("\001");
            resultMap = new HashMap<>();
            resultMap.put("id", detail[0]);
            resultMap.put("name", detail[1]);
            resultMap.put("sex", detail[2]);
            resultMap.put("city", detail[3]);
            resultMap.put("occupation", detail[4]);
            resultMap.put("mobile_phone_num", detail[5]);
            resultMap.put("fix_phone_num", detail[6]);
            resultMap.put("bank_name", detail[7]);
            resultMap.put("address", detail[8]);
            resultMap.put("marriage", detail[9]);
            resultMap.put("child_num", detail[10]);
            return JSONObject.fromObject(resultMap).toString();
        }
        return null;
    }

    /**
     * 加载Kafka公共配置
     */
    private static  Properties getPublicProps() {
        Properties properties = new Properties();
        try{
            File file = new File(confPath + File.separator + "0286.properties");

            if(!file.exists()) {
                InputStream in = ReadUserInfoFileToKafka_0286.class.getClassLoader().getResourceAsStream("0286.properties");
                properties.load(in);
            } else {
                properties.load(new FileInputStream(file));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return properties;
    }

    /**
     * 初始化Kafka配置
     * @return
     */
    private static Properties getKafkaProps() {
        try{
            Properties properties = new Properties();
            properties.put(ProducerConfig.ACKS_CONFIG, "all");
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000); //批量发送消息
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            return properties;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
