package com.dbs.artemis.service;

import com.alibaba.excel.EasyExcel;
import com.dbs.artemis.listener.EventLogReplayListener;
import com.dbs.artemis.model.SparkEventLog;
import com.dbs.artemis.model.SparkMetrics;
import com.dbs.artemis.model.SparkTask;
import com.dbs.artemis.model.TaskMetrics;
import com.dbs.artemis.util.ResourceUtil;
import com.dbs.artemis.util.StreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.ReplayListenerBus;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.function.ToLongFunction;

@Slf4j
@Component
public class EventLogReader {

    public void read() {
        try {
            File logs = new File(getClass().getClassLoader().getResource("even-logs").toURI());
            File[] files = logs.listFiles();
            if (!logs.exists() || files == null) {
                log.info("logs not exists!");
                return;
            }
            List<SparkMetrics> metricsList = new ArrayList<>();
            for (File file : files) {
                InputStream inputStream = new FileInputStream(file);
                EventLogReplayListener listener = new EventLogReplayListener(SparkEventLog.builder().build());
                ReplayListenerBus bus = new ReplayListenerBus();
                bus.addListener(listener);
                bus.replay(inputStream, file.getName(), false, ReplayListenerBus.SELECT_ALL_FILTER());
                SparkMetrics metrics = buildMetrics(listener.getEventLog());
                log.info("name: {}, metrics: {}", file.getName(), metrics);
                metricsList.add(metrics);
            }
            String outputPath = "D:\\java\\IdeaProjects\\Artemis\\src\\main\\resources\\output\\Metrics" + System.currentTimeMillis()+".csv";
            EasyExcel.write(outputPath, SparkMetrics.class).sheet("Metrics").doWrite(metricsList);
        } catch (Exception e) {
            log.error("read error", e);
        }
    }

    private SparkMetrics buildMetrics(SparkEventLog eventLog) {
        SparkMetrics sparkMetrics = SparkMetrics.builder().build();
        sparkMetrics.setApplicationId(eventLog.getApplicationId());
        sparkMetrics.setStageCount(countStage(eventLog));
        sparkMetrics.setTaskCount(countTask(eventLog));
        sparkMetrics.setJobCount(countJob(eventLog));
        sparkMetrics.setDuration(eventLog.getEndTime() - eventLog.getStartTime());
        sparkMetrics.setVCore(eventLog.getSparkConfig().get("spark.executor.vcores"));
        sparkMetrics.setMemory(eventLog.getSparkConfig().get("spark.executor.memory"));
        sparkMetrics.setTotalShuffleReadBytes(sum(eventLog, TaskMetrics::getShuffleReadBytes));
        sparkMetrics.setTotalShuffleByteWritten(sum(eventLog, TaskMetrics::getShuffleByteWritten));
        sparkMetrics.setExecutorDeserializeTime(sum(eventLog, TaskMetrics::getExecutorDeserializeTime));
        sparkMetrics.setExecutorDeserializeCpuTime(sum(eventLog, TaskMetrics::getExecutorDeserializeCpuTime));
        sparkMetrics.setResultSerializeTime(sum(eventLog, TaskMetrics::getResultSerializeTime));
        sparkMetrics.setMemoryBytesSpill(sum(eventLog, TaskMetrics::getMemoryBytesSpill));
        sparkMetrics.setDiskBytesSpill(sum(eventLog, TaskMetrics::getDiskBytesSpill));
        sparkMetrics.setInputBytesRead(sum(eventLog, TaskMetrics::getInputBytesRead));
        sparkMetrics.setOutputBytesWritten(sum(eventLog, TaskMetrics::getOutputBytesWritten));
        sparkMetrics.setJvmGcTime(sum(eventLog, TaskMetrics::getJvmGcTime));
        sparkMetrics.setPeakExecutionMemory(max(eventLog, TaskMetrics::getPeakExecutionMemory, Long::compareTo));
        return sparkMetrics;
    }

    private Integer countJob(SparkEventLog eventLog) {
        return Optional.ofNullable(eventLog.getJobs()).orElse(new ArrayList<>()).size();
    }

    private Integer countStage(SparkEventLog eventLog) {
        return (int) StreamUtil.of(eventLog.getJobs()).flatMap(job -> StreamUtil.of(job.getStages())).count();
    }

    private Integer countTask(SparkEventLog eventLog) {
        return (int) StreamUtil.of(eventLog.getJobs()).flatMap(job -> StreamUtil.of(job.getStages()))
                .flatMap(stage -> StreamUtil.of(stage.getTasks())).count();
    }

    private Long sum(SparkEventLog eventLog, ToLongFunction<? super TaskMetrics> function) {
        return StreamUtil.of(eventLog.getJobs()).flatMap(job -> StreamUtil.of(job.getStages()))
                .flatMap(stage -> StreamUtil.of(stage.getTasks())).map(SparkTask::getMetrics)
                .filter(Objects::nonNull).mapToLong(function).sum();
    }

    private Long max(SparkEventLog eventLog, Function<? super TaskMetrics, Long> function, Comparator<Long> comparator) {
        return StreamUtil.of(eventLog.getJobs()).flatMap(job -> StreamUtil.of(job.getStages()))
                .flatMap(stage -> StreamUtil.of(stage.getTasks())).map(SparkTask::getMetrics)
                .filter(Objects::nonNull).map(function).max(comparator).orElse(0L);
    }
}
