package com.dbs.artemis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkMetrics {

    private String applicationId;

    private String status;

    private Integer stageCount;

    private Integer failedStageCount;

    private Integer taskCount;

    private Integer jobCount;

    private Long duration;

    private String vCore;

    private String memory;

    private Long totalShuffleReadBytes;

    private Long totalShuffleByteWritten;

    private Long executorDeserializeTime;

    private Long executorDeserializeCpuTime;

    private Long resultSerializeTime;

    private Long memoryBytesSpill;

    private Long diskBytesSpill;

    private Long inputBytesSpill;

    private Long outputBytesWritten;

    private Long jvmGcTime;

    private Long peakJvmMemory;

    private Long peakExecutionMemory;

    private Long peakStorageMemory;
}
