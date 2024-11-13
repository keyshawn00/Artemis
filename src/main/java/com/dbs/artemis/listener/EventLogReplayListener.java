package com.dbs.artemis.listener;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.dbs.artemis.model.*;
import com.dbs.artemis.util.StreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Event order:
 * onEnvironmentUpdate
 * onApplicationStart
 * onExecutorAdded
 * onJobStart
 * onStageSubmitted
 * onTaskStart
 * onTaskEnd
 * onStageCompleted
 * onJobEnd
 * onExecutorRemoved
 * onApplicationEnd
 */
@Slf4j
public class EventLogReplayListener extends SparkListener {

    private SparkEventLog eventLog;

    private EventLogReplayListener() {
    }

    public SparkEventLog getEventLog() {
        return eventLog;
    }

    public EventLogReplayListener(SparkEventLog eventLog) {
        this.eventLog = eventLog;
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
        log.info("onEnvironmentUpdate");
        Map<String, String> configMap = MapUtil.newHashMap();
        environmentUpdate.environmentDetails().get("Spark Properties")
                .get().foreach(entry -> configMap.put(entry._1(), entry._2()));
        eventLog.setSparkConfig(configMap);
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log.info("onApplicationStart");
        eventLog.setApplicationId(applicationStart.appId().get());
        eventLog.setStartTime(applicationStart.time());
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        log.info("onApplicationEnd");
        eventLog.setEndTime(applicationEnd.time());
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        log.info("onJobStart");
        List<Integer> stageIds = new ArrayList<>();
        jobStart.stageInfos().foreach(stage -> stageIds.add(stage.stageId()));
        SparkJobVO job = SparkJobVO.builder()
                .id(jobStart.jobId())
                .startTime(jobStart.time())
                .stageIds(stageIds)
                .build();
        List<SparkJobVO> jobs = Optional.ofNullable(eventLog.getJobs()).orElse(new ArrayList<>());
        jobs.add(job);
        eventLog.setJobs(jobs);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        log.info("onJobEnd");
        Map<Integer, SparkJobVO> jobMap =
                StreamUtil.toMap(eventLog.getJobs(), SparkJobVO::getId, Function.identity());
        SparkJobVO sparkJob = jobMap.get(jobEnd.jobId());
        Optional.ofNullable(sparkJob).ifPresent(job -> job.setEnTime(jobEnd.time()));
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        log.info("onStageSubmitted");
        SparkJobVO sparkJob = findSparkJob(stageSubmitted.stageInfo().stageId());
        SparkStage stage = SparkStage.builder()
                .id(stageSubmitted.stageInfo().stageId())
                .startTime((Long) stageSubmitted.stageInfo().submissionTime().get())
                .build();

        Optional.ofNullable(sparkJob).ifPresent(job -> {
            List<SparkStage> stages = Optional.ofNullable(job.getStages()).orElse(new ArrayList<>());
            stages.add(stage);
            job.setStages(stages);
        });
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        log.info("onStageCompleted");
        Integer stageId = stageCompleted.stageInfo().stageId();
        Map<Integer, SparkStage> stageMap = getStageMap();
        SparkStage sparkStage = stageMap.get(stageId);
        Optional.ofNullable(sparkStage).ifPresent(stage ->
                stage.setEndTime((Long) stageCompleted.stageInfo().completionTime().get()));
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        log.info("onTaskStart");
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        log.info("onTaskEnd");
        TaskInfo taskInfo = taskEnd.taskInfo();
        if (taskInfo == null) {
            return;
        }
        Map<Integer, SparkStage> stageMap = getStageMap();
        SparkStage sparkStage = stageMap.get(taskEnd.stageId());
        Optional.ofNullable(sparkStage).ifPresent(stage -> {
            List<SparkTask> tasks = Optional.ofNullable(stage.getTasks()).orElse(new ArrayList<>());
            tasks.add(buildSparkTask(taskInfo, taskEnd.taskMetrics()));
            stage.setTasks(tasks);
        });
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        log.info("onExecutorAdded");
        ExecutorInfo executorInfo = executorAdded.executorInfo();
        SparkExecutor executor = SparkExecutor.builder()
                .id(executorAdded.executorId())
                .cores(executorInfo.totalCores())
                .startTime(executorAdded.time())
                .build();
        List<SparkExecutor> executors = Optional.ofNullable(eventLog.getExecutors())
                .orElse(new ArrayList<>());
        executors.add(executor);
        eventLog.setExecutors(executors);
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        log.info("onExecutorRemoved");
        SparkExecutor executor = StreamUtil.findFirst(eventLog.getExecutors(), e ->
                Objects.equals(e.getId(), executorRemoved.executorId()), null);
        Optional.ofNullable(executor).ifPresent(e -> {
            e.setEndTime(executorRemoved.time());
            e.setDuration(e.getEndTime() - e.getStartTime());
        });
    }

    private Map<Integer, SparkStage> getStageMap() {
        return StreamUtil.of(eventLog.getJobs()).map(SparkJobVO::getStages)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(SparkStage::getId, Function.identity()));
    }

    private SparkJobVO findSparkJob(Integer stageId) {
        return StreamUtil.findFirst(eventLog.getJobs(),
                job -> {
                    if (CollUtil.isEmpty(job.getStageIds())) {
                        return false;
                    }
                    return job.getStageIds().contains(stageId);
                }, null);
    }

    private SparkTask buildSparkTask(TaskInfo taskInfo, TaskMetrics taskMetrics) {
        SparkTask task = SparkTask.builder()
                .taskId(taskInfo.taskId())
                .finishTime(taskInfo.finishTime())
                .executorId(taskInfo.executorId())
                .launchTime(taskInfo.launchTime())
                .failed(taskInfo.failed())
                .killed(taskInfo.killed())
                .finished(taskInfo.finished())
                .build();
        fillTaskMetrics(task, taskMetrics);
        return task;
    }

    private void fillTaskMetrics(SparkTask task, TaskMetrics taskMetrics) {
        if (taskMetrics == null) {
            return;
        }
        com.dbs.artemis.model.TaskMetrics metrics = new com.dbs.artemis.model.TaskMetrics();
        metrics.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        metrics.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        metrics.setJvmGcTime(taskMetrics.jvmGCTime());
        metrics.setResultSerializeTime(taskMetrics.resultSerializationTime());
        metrics.setMemoryBytesSpill(taskMetrics.memoryBytesSpilled());
        metrics.setDiskBytesSpill(taskMetrics.diskBytesSpilled());
        metrics.setPeakJvmMemory(0L);
        metrics.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        metrics.setPeakStorageMemory(0L);
        metrics.setShuffleReadBytes(getShuffleReadBytes(taskMetrics));
        metrics.setShuffleByteWritten(getShuffleByteWritten(taskMetrics));
        task.setMetrics(metrics);
    }

    private Long getShuffleReadBytes(TaskMetrics taskMetrics) {
        if (taskMetrics.shuffleReadMetrics() == null) {
            return 0L;
        }
        return taskMetrics.shuffleReadMetrics().localBytesRead() +
                taskMetrics.shuffleReadMetrics().remoteBytesRead();
    }

    private Long getShuffleByteWritten(TaskMetrics taskMetrics) {
        if (taskMetrics.shuffleWriteMetrics() == null) {
            return 0L;
        }
        return taskMetrics.shuffleWriteMetrics().bytesWritten();
    }
}
