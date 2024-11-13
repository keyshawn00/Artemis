package com.dbs.artemis.service;

import com.dbs.artemis.listener.EventLogReplayListener;
import com.dbs.artemis.model.SparkEventLog;
import com.dbs.artemis.model.SparkMetrics;
import com.dbs.artemis.util.ResourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.ReplayListenerBus;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Slf4j
@Component
public class EventLogReader {

    public void read() {
        EventLogReplayListener listener = new EventLogReplayListener(SparkEventLog.builder().build());
        ReplayListenerBus bus = new ReplayListenerBus();
        bus.addListener(listener);
        String path = "even-logs/application_1731405959099_0007";
        InputStream inputStream = ResourceUtil.getResource(path);
        bus.replay(inputStream, "", false, ReplayListenerBus.SELECT_ALL_FILTER());

        SparkMetrics metrics = buildMetrics(listener.getEventLog());
        log.info("metrics: {}", metrics);
    }

    private SparkMetrics buildMetrics(SparkEventLog eventLog) {
        return SparkMetrics.builder().build();
    }
}
