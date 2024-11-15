package com.dbs.artemis.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskMetrics {

    private Long executorDeserializeTime;

    private Long executorDeserializeCpuTime;

    private Long jvmGcTime;

    private Long resultSerializeTime;

    private Long memoryBytesSpill;

    private Long diskBytesSpill;

    private Long peakExecutionMemory;

    private Long shuffleReadBytes;

    private Long shuffleByteWritten;

    private Long inputBytesRead;

    private Long outputBytesWritten;
}