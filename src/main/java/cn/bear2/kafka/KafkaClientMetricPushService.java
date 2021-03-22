package cn.bear2.kafka;

import io.prometheus.client.Collector;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus pushGateway service for kafka-client like consumer,producer.
 */
public final class KafkaClientMetricPushService {
    public KafkaClientMetricPushService(List<Object> clientList, String pushGateWayServer, String jobName, long initialDelay, long period) {
        Collector register = new KafkaClientMetricCollector(clientList).register();
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(new PushScheduledExecutor(pushGateWayServer,register,jobName), initialDelay, period, TimeUnit.SECONDS);
    }

    // 默认延迟15s,每60s上报一次指标
    public KafkaClientMetricPushService(List<Object> clientList, String pushGateWayServer, String jobName) {
        Collector register = new KafkaClientMetricCollector(clientList).register();
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(new PushScheduledExecutor(pushGateWayServer,register,jobName), 15, 60, TimeUnit.SECONDS);
    }
}

class PushScheduledExecutor implements Runnable {
    private final PushGateway pg;
    private final Collector register;
    private final String jobName;

    public PushScheduledExecutor(String pushGateWayServer,Collector register,String jobName) {
        this.pg = new PushGateway(pushGateWayServer);
        this.register = register;
        this.jobName = jobName;
    }

    @Override
    public void run() {
        try {
            pg.push(register, jobName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
