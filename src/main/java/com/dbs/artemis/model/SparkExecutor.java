package com.dbs.artemis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkExecutor {

    private String id;

    private Long startTime;

    private Long endTime;

    private Long duration;

    private Integer cores;
}
