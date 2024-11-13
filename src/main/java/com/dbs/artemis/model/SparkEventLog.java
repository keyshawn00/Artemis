package com.dbs.artemis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkEventLog {

    private String applicationId;

    private Long startTime;

    private Long endTime;

    private Map<String, String> sparkConfig;

    private List<SparkExecutor> executors;

    private List<SparkJobVO> jobs;
}
