package com.dbs.artemis.service;

import com.dbs.artemis.listener.EventLogReplayListener;
import com.dbs.artemis.model.SparkMetricsVO;
import com.dbs.artemis.util.ResourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.ReplayListenerBus;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Slf4j
@Component
public class EventLogReader {

    public void read() {
        EventLogReplayListener listener = new EventLogReplayListener(SparkMetricsVO.builder().build());
        ReplayListenerBus bus = new ReplayListenerBus();
        bus.addListener(listener);
        String path = "";
        InputStream inputStream = ResourceUtil.getResource(path);
        bus.replay(inputStream, "", false, ReplayListenerBus.SELECT_ALL_FILTER());
        log.info("metrics: {}", listener.getMetrics());
    }
}
