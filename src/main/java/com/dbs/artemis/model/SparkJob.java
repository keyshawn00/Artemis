package com.dbs.artemis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkJob {

    private Integer id;

    private Long startTime;

    private Long enTime;

    private List<Integer> stageIds;

    private List<SparkStage> stages;
}
