package test.cn.bear2.kafka;

import cn.bear2.kafka.KafkaClientMetricCollector;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestKafkaProducerMetricCollector {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("kafkaServer"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        Producer<Integer, String> producer2 = new KafkaProducer<Integer, String>(props);
        String topic = "kafka_test";

        DefaultExports.initialize();
        List<Object> arrayList = new ArrayList();
        arrayList.add(producer);
        arrayList.add(producer2);
        new KafkaClientMetricCollector(arrayList).register();
        HTTPServer server = new HTTPServer(8033);

        while (true) {
            Thread.sleep(100);
            producer.send(new ProducerRecord<Integer, String>(topic,
                    0,
                    "kafka_test|b1|t1|"));
        }

    }
}
