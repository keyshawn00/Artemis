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
        Map<String, String> configMap = MapUtil.newHashMap();
        environmentUpdate.environmentDetails().get("Spark Properties")
                .get().foreach(entry -> configMap.put(entry._1(), entry._2()));
        eventLog.setSparkConfig(configMap);
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        eventLog.setApplicationId(applicationStart.appId().get());
        eventLog.setStartTime(applicationStart.time());
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        eventLog.setEndTime(applicationEnd.time());
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        List<Integer> stageIds = new ArrayList<>();
        jobStart.stageInfos().foreach(stage -> stageIds.add(stage.stageId()));
        SparkJob job = SparkJob.builder()
                .id(jobStart.jobId())
                .startTime(jobStart.time())
                .stageIds(stageIds)
                .build();
        List<SparkJob> jobs = Optional.ofNullable(eventLog.getJobs()).orElse(new ArrayList<>());
        jobs.add(job);
        eventLog.setJobs(jobs);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        Map<Integer, SparkJob> jobMap =
                StreamUtil.toMap(eventLog.getJobs(), SparkJob::getId, Function.identity());
        SparkJob sparkJob = jobMap.get(jobEnd.jobId());
        Optional.ofNullable(sparkJob).ifPresent(job -> job.setEnTime(jobEnd.time()));
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        SparkJob sparkJob = findSparkJob(stageSubmitted.stageInfo().stageId());
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
        Integer stageId = stageCompleted.stageInfo().stageId();
        Map<Integer, SparkStage> stageMap = getStageMap();
        SparkStage sparkStage = stageMap.get(stageId);
        Optional.ofNullable(sparkStage).ifPresent(stage ->
                stage.setEndTime((Long) stageCompleted.stageInfo().completionTime().get()));
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
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
        SparkExecutor executor = StreamUtil.findFirst(eventLog.getExecutors(), e ->
                Objects.equals(e.getId(), executorRemoved.executorId()), null);
        Optional.ofNullable(executor).ifPresent(e -> {
            e.setEndTime(executorRemoved.time());
            e.setDuration(e.getEndTime() - e.getStartTime());
        });
    }

    private Map<Integer, SparkStage> getStageMap() {
        return StreamUtil.of(eventLog.getJobs()).map(SparkJob::getStages)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(SparkStage::getId, Function.identity()));
    }

    private SparkJob findSparkJob(Integer stageId) {
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
        metrics.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        metrics.setShuffleReadBytes(getShuffleReadBytes(taskMetrics));
        metrics.setShuffleByteWritten(getShuffleByteWritten(taskMetrics));
        metrics.setInputBytesRead(getInputBytesRead(taskMetrics));
        metrics.setOutputBytesWritten(getOutputBytesWritten(taskMetrics));
        task.setMetrics(metrics);
    }

    private Long getOutputBytesWritten(TaskMetrics taskMetrics) {
        if (taskMetrics.outputMetrics() == null) {
            return 0L;
        }
        return taskMetrics.outputMetrics().bytesWritten();
    }

    private Long getInputBytesRead(TaskMetrics taskMetrics) {
        if (taskMetrics.inputMetrics() == null) {
            return 0L;
        }
        return taskMetrics.inputMetrics().bytesRead();
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
