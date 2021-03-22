package cn.bear2.kafka.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trans kafka-clients`s metrics to prometheus format.
 */
public class KafkaClientMetrics {
    private List<Map<MetricName, ? extends Metric>> clientMetricsList = new ArrayList<>();

    public KafkaClientMetrics(List<Object> clientList) {
        for(Object client:clientList){
            if (client instanceof Producer) {
                clientMetricsList.add(((Producer) client).metrics());
            } else if (client instanceof Consumer) {
                clientMetricsList.add(((Consumer) client).metrics());
            } else {
                throw new IllegalArgumentException("Not support client");
            }
        }
    }

    // trans jmx-name 2 prometheus-name
    private String metricNameTrans(KafkaMetric metric) {
        // 基础转换
        String baseMetricName = String.format("%s_%s", metric.metricName().group(), metric.metricName().name())
                .replaceAll("\\.", "_")
                .replaceAll("-", "_");
        // 取决于开发者规范，依据描述的值
        if (metric.metricName().description().contains("for a topic")) {
            // eg: consumer_fetch_manager_metrics_records_per_request_avgThe average number of records in each request for a topic
            return baseMetricName + "_" + "by_topic";
        } else if (metric.metricName().description().contains("the partition")) {
            // eg: consumer_fetch_manager_metrics_records_lag_max     The max lag of the partition
            return baseMetricName + "_" + "by_partition";
        } else {
            return baseMetricName;
        }
    }

    // trans jmx-tag 2 prometheus-label
    private Map<String, List<String>> tag2label(Map<String, String> tag) {
        Map<String, List<String>> labelMap = new HashMap<>();
        List<String> labelKey = new ArrayList<>();
        List<String> labelValue = new ArrayList<>();

        tag.forEach((key, value) -> {
            labelKey.add(key.replaceAll("-", "_"));
            labelValue.add(value);
        });
        labelMap.put("labelKey", labelKey);
        labelMap.put("labelValue", labelValue);
        return labelMap;
    }

    public List<Collector.MetricFamilySamples> getMetrics() {
        Map<String, Collector.MetricFamilySamples> prometheusMetric = new HashMap<>();
        for(Map<MetricName, ? extends Metric> clientMetrics:clientMetricsList){
            clientMetrics.forEach(
                    ((metricName, metric) -> {
                        Map<String, List<String>> label = tag2label(metric.metricName().tags());
                        List<String> labelKey = label.get("labelKey");
                        List<String> labelValue = label.get("labelValue");
                        KafkaMetric kafkaMetric = (KafkaMetric) metric;
                        String prometheusMetricName = metricNameTrans(kafkaMetric);
                        try {
                            // 为了兼容0.9.0.1b版本,目前使用value
                            double value = metric.value();
                            if (metricName.name().endsWith("total")) {
                                // 存在这样的场景
                                // request-size-avg	42	The average size of all requests in the window..	{client-id=DemoProducer, node-id=node--1}
                                // request-size-avg	256	The average size of all requests in the window..	{client-id=DemoProducer, node-id=node-12140}
                                // request-size-avg	246.75	The average size of all requests in the window..	{client-id=DemoProducer, node-id=node-12139}
                                if (prometheusMetric.containsKey(prometheusMetricName)) {
                                    CounterMetricFamily counterMetricFamily = (CounterMetricFamily) prometheusMetric.get(prometheusMetricName);
                                    counterMetricFamily.addMetric(labelValue, value);
                                } else {
                                    CounterMetricFamily counterMetricFamily = new CounterMetricFamily(prometheusMetricName, ((KafkaMetric) metric).metricName().description(), labelKey);
                                    counterMetricFamily.addMetric(labelValue, value);
                                    prometheusMetric.put(prometheusMetricName, counterMetricFamily);
                                }
                            } else{
                                // 其他默认是gauge类型
                                if (prometheusMetric.containsKey(prometheusMetricName)) {
                                    GaugeMetricFamily gaugeMetricFamily = (GaugeMetricFamily) prometheusMetric.get(prometheusMetricName);
                                    gaugeMetricFamily.addMetric(labelValue, value);
                                } else {
                                    GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily(prometheusMetricName, ((KafkaMetric) metric).metricName().description(), labelKey);
                                    gaugeMetricFamily.addMetric(labelValue, value);
                                    prometheusMetric.put(prometheusMetricName, gaugeMetricFamily);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    })
            );
        }
        return new ArrayList<>(prometheusMetric.values());
    }
}
