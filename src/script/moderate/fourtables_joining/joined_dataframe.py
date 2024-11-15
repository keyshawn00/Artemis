from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta

# 创建SparkSession
spark = SparkSession.builder.appName("4Tables_joined_Dataframe").getOrCreate()




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
num_rows_evnen = 100000
num_columns_evnen = 100
df_even = create_even_dataframe(num_rows_evnen, num_columns_evnen)
df_even2 = create_even_dataframe(num_rows_evnen, num_columns_evnen)
df_even3 = create_even_dataframe(num_rows_evnen, num_columns_evnen)

def create_uneven_dataframe(num_rows, num_columns):
    # 创建 schema，包括"business_date"字段
    schema = StructType([StructField(f"column_{i}", StringType(), True) for i in range(1, num_columns + 1)])
    schema.add(StructField("business_date", StringType(), True))

    # 设置主要日期范围和少数日期范围
    main_date_start = datetime(2024, 2, 1)
    main_date_end = datetime(2024, 6, 30)
    secondary_date_start = datetime(2024, 1, 1)
    secondary_date_end = datetime(2024, 12, 31)  # 整个一年内的分布

    # 生成数据，包括不均匀分布的 "business_date"
    data = []
    for _ in range(num_rows):
        row = [f"value_{random.randint(1, 100)}" for _ in range(num_columns)]

        # 随机选择主要日期范围的概率为 80%，少数日期范围的概率为 20%
        if random.random() < 0.8:
            random_days = random.randint(0, (main_date_end - main_date_start).days)
            business_date = (main_date_start + timedelta(days=random_days)).strftime("%Y-%m-%d")
        else:
            random_days = random.randint(0, (secondary_date_end - secondary_date_start).days)
            business_date = (secondary_date_start + timedelta(days=random_days)).strftime("%Y-%m-%d")

        row.append(business_date)
        data.append(tuple(row))

    # 创建 DataFrame
    df = spark.createDataFrame(data, schema)
    return df

# 指定行数和列数
num_rows_uneven = 100000
num_columns_uneven = 100
df_uneven = create_uneven_dataframe(num_rows_uneven, num_columns_uneven)

def joined_dataframe(df1,df2,df3,df4):
  df = df1.join(df2, on='business_date', how='inner')
  df_joined1 = df.join(df3,on='business_date', how='inner')
  df_joined2 = df_joined1.join(df4,on='business_date', how='inner')

  return df_joined2


df_final = joined_dataframe(df_uneven,df_even,df_even2,df_even3)
df_final.show()
