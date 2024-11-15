from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta

# 创建SparkSession
spark = SparkSession.builder.appName("10small_files") \
        .config("spark.sql.adaptive.coalescePartitions.enabled","false") \
        .config("spark.sql.adaptive.enabled","false") \
        .getOrCreate()


def create_even_dataframe(num_rows, num_columns):
    # 创建schema，包括"business_date"字段
    schema = StructType([StructField(f"column_{i}", StringType(), True) for i in range(1, num_columns + 1)])
    schema.add(StructField("business_date", StringType(), True))

    # 生成数据，包括随机的"business_date"
    data = []
    start_date = datetime(2024, 1, 1)
    for _ in range(num_rows):
        row = [f"value_{random.randint(1, 100)}" for _ in range(num_columns)]
        business_date = (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
        row.append(business_date)
        data.append(tuple(row))

    # 创建DataFrame
    df = spark.createDataFrame(data, schema)
    return df

# 指定行数和列数
num_rows = 1000
num_columns = 100
df = create_even_dataframe(num_rows, num_columns)

#写3次读3次



for i in range(0,3):
    num_partitions = 100
    df_repartitioned = df.repartition(num_partitions)
    output_path = f"oss://buckethackthon08/w-d9cc27799560b7df/spark/data/small_files{i}"
    df_repartitioned.write.mode("overwrite").option("header", "true").save(output_path)

list1 = []
for i in range(0,3):
    input_path = f"oss://buckethackthon08/w-d9cc27799560b7df/spark/data/small_files{i}"
    df = spark.read.parquet(input_path)
    list1.append(df)

