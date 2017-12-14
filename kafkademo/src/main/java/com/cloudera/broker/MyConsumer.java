package com.cloudera.broker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * package: com.cloudera
 * describe: Kerberos环境下通过Kafka的Bootstrap.Server消费数据
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2017/12/12
 * creat_time: 下午3:35
 * 公众号：Hadoop实操
 */
public class MyConsumer {

    private static String TOPIC_NAME = "test3";

    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/jaas-cache.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "true");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-31-21-45.ap-southeast-1.compute.internal:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition partition1 = new TopicPartition(TOPIC_NAME, 1);
        TopicPartition partition2 = new TopicPartition(TOPIC_NAME, 2);

        consumer.assign(Arrays.asList(partition0, partition1, partition2));

        ConsumerRecords<String, String> records = null;

        while (true) {
            try {
                Thread.sleep(10000l);
                System.out.println();
                records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
