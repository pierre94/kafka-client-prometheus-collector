package test.cn.bear2.kafka;

import cn.bear2.kafka.KafkaClientMetricPushService;

import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TestKafkaConsumerMetricCollector {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("kafkaServer"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        KafkaConsumer<Integer, String> consumer2 = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("kafka_test"));
        consumer2.subscribe(Collections.singletonList("kafka_test"));

        List<Object> clientList = new ArrayList<>();
        clientList.add(consumer);
        clientList.add(consumer2);

        // pushGateway形式
        new KafkaClientMetricPushService(clientList, System.getenv("pushGateWayServer"), "my_kafka_client");

        // http exporter形式
        // Collector register = new KafkaClientMetricCollector(clientList).register();
        // HTTPServer server = new HTTPServer(8031);

        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                // System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
        }
    }
}



