package cn.bear2.kafka;

import cn.bear2.kafka.metrics.KafkaClientMetrics;
import io.prometheus.client.Collector;

import java.util.List;

/**
 * Prometheus collector for kafka-client like consumer,producer.
 */
public class KafkaClientMetricCollector extends Collector {
    private final KafkaClientMetrics kafkaClientMetrics;

    public KafkaClientMetricCollector(List<Object> kafkaClients) {
        kafkaClientMetrics = new KafkaClientMetrics(kafkaClients);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return kafkaClientMetrics.getMetrics();
    }
}
