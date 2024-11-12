package com.dbs.artemis.listener;

import com.dbs.artemis.model.SparkMetricsVO;
import org.apache.spark.scheduler.*;

public class EventLogReplayListener extends SparkListener {

    private SparkMetricsVO metrics;

    private EventLogReplayListener() {
    }

    public SparkMetricsVO getMetrics() {
        return metrics;
    }

    public EventLogReplayListener(SparkMetricsVO metrics) {
        this.metrics = metrics;
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {

    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {

    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {

    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {

    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {

    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {

    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {

    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {

    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {

    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {

    }
}
