package com.cloudera.nokerberos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * package: com.cloudera
 * describe: 非Kerberos环境下向指定Topic生产消息
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/15
 * creat_time: 下午11:38
 * 公众号：Hadoop实操
 */
public class NoneKBProducerTest {

    public static String confPath = System.getProperty("user.dir") + File.separator + "conf";

    public static void main(String[] args) {
        try {
            Properties appProperties = new Properties();
            appProperties.load(new FileInputStream(new File(confPath + File.separator + "app.properties")));

            String brokerlist = String.valueOf(appProperties.get("bootstrap.servers"));
            String topic_name = String.valueOf(appProperties.get("topic.name"));

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            for (int i = 0; i < 10; i++) {
                String message = i + "\t" + "fayson" + i  + "\t" + 22+i;
                ProducerRecord record = new ProducerRecord<String, String>(topic_name, message);
                producer.send(record);
                System.out.println(message);
            }

            producer.flush();
            producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}