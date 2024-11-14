package com.dbs.artemis.model;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkMetrics {

    @ExcelProperty("applicationId")
    private String applicationId;

    @ExcelProperty("stageCount")
    private Integer stageCount;

    @ExcelProperty("taskCount")
    private Integer taskCount;

    @ExcelProperty("jobCount")
    private Integer jobCount;

    @ExcelProperty("duration")
    private Long duration;

    @ExcelProperty("vCore")
    private String vCore;

    @ExcelProperty("memory")
    private String memory;

    @ExcelProperty("totalShuffleReadBytes")
    private Long totalShuffleReadBytes;

    @ExcelProperty("totalShuffleByteWritten")
    private Long totalShuffleByteWritten;

    @ExcelProperty("executorDeserializeTime")
    private Long executorDeserializeTime;

    @ExcelProperty("executorDeserializeCpuTime")
    private Long executorDeserializeCpuTime;

    @ExcelProperty("resultSerializeTime")
    private Long resultSerializeTime;

    @ExcelProperty("memoryBytesSpill")
    private Long memoryBytesSpill;

    @ExcelProperty("diskBytesSpill")
    private Long diskBytesSpill;

    @ExcelProperty("inputBytesRead")
    private Long inputBytesRead;

    @ExcelProperty("outputBytesWritten")
    private Long outputBytesWritten;

    @ExcelProperty("jvmGcTime")
    private Long jvmGcTime;

    @ExcelProperty("peakExecutionMemory")
    private Long peakExecutionMemory;

}
