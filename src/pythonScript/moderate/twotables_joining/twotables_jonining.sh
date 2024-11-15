#!/bin/bash

# 定义每次提交的参数
DRIVER_MEMORY=("2G" "2G" "2G" "3G" "3G")
EXECUTOR_MEMORY=("4G" "6G" "8G" "10G" "12G")
EXECUTOR_CORES=(1 2 3 4 5)
NUM_EXECUTORS=(2 2 2 3 4)

# Spark应用程序的主类和jar包路径
MAIN_CLASS="class org.apache.spark.tables_joining"
APP_JAR="/root/test/moderate/twotables_joining/joined_dataframe.py"

# 提交Spark作业的循环
for i in {0..4}; do
  echo "Submitting job #$((i+1)) with parameters:"
  echo "Driver Memory: ${DRIVER_MEMORY[i]}"
  echo "Executor Memory: ${EXECUTOR_MEMORY[i]}"
  echo "Executor Cores: ${EXECUTOR_CORES[i]}"
  echo "Num Executors: ${NUM_EXECUTORS[i]}"

  spark-submit \
    --class "$MAIN_CLASS" \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory "${DRIVER_MEMORY[i]}" \
    --executor-memory "${EXECUTOR_MEMORY[i]}" \
    --executor-cores "${EXECUTOR_CORES[i]}" \
    --num-executors "${NUM_EXECUTORS[i]}" \
    "$APP_JAR"

  echo "Job #$((i+1)) submitted."
  echo ""
done
