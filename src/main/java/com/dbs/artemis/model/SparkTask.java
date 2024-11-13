package com.dbs.artemis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkTask {

    private Long taskId;

    private String executorId;

    private Long launchTime;

    private  Long finishTime;

    private Boolean failed;

    private Boolean killed;

    private Boolean finished;

    private TaskMetrics metrics;
}
