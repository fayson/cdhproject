package com.cloudera.kerberos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by fayson on 2017/10/24.
 * 主要是用于向
 */
public class MyProducer {

    public static String TOPIC_NAME = "test3";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/jaas-cache.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-31-21-45.ap-southeast-1.compute.internal:9092,ip-172-31-26-102.ap-southeast-1.compute.internal:9020,ip-172-31-26-80.ap-southeast-1.compute.internal:9020");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            String key = "keyNEW-" + i;
            String message = "MessageNEW-" + i;
            ProducerRecord record = new ProducerRecord<String, String>(TOPIC_NAME, key, message);
            producer.send(record);
            System.out.println(key + "----" + message);
        }

        producer.flush();
        producer.close();
    }
}
