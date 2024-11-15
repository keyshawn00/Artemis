#!/bin/bash

# 定义每次提交的参数
DRIVER_MEMORY=("1G" "2G" "3G" "4G" "5G")
EXECUTOR_MEMORY=("2G" "3G" "3g" "3g" "4G")
EXECUTOR_CORES=(1 2 3 4 5)
NUM_EXECUTORS=(2 4 5 6 8)

# Spark应用程序的主类和jar包路径
MAIN_CLASS="class org.apache.spark.thirtytypartition"
APP_JAR="/root/test/io/thirtytypartition/iotype.py"

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
