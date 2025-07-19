from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import lit


def get_spark_session():
    """初始化SparkSession，启用Hive支持"""
    spark = SparkSession.builder \
       .appName("DWD_Table_Copy_With_New_Dt") \
       .config("hive.metastore.uris", "thrift://cdh01:9083") \
       .config("spark.sql.hive.convertMetastoreOrc", "true") \
       .config("spark.driver.memory", "4g") \
       .config("spark.executor.memory", "4g") \
       .config("spark.network.timeout", "600s") \
       .config("spark.executor.heartbeatInterval", "300s") \
       .config("hive.exec.dynamic.partition.mode", "nonstrict")  \
       .enableHiveSupport() \
       .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE gmall")
    return spark


def check_partition_exists(spark, table_name, dt_value):
    """检查目标表的dt分区是否已存在"""
    try:
        # 直接查询表中是否有该分区的数据
        count = spark.sql(f"""
            SELECT 1 FROM {table_name} 
            WHERE dt = '{dt_value}' 
            LIMIT 1
        """).count()
        return count > 0
    except Exception as e:
        print(f"[WARN] 检查表{table_name}分区{dt_value}时出错: {str(e)}")
        return False


def get_all_partitions(spark, table_name):
    """获取表中所有的dt分区值"""
    try:
        partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
        dt_partitions = []
        for row in partitions_df.collect():
            partition_str = row[0]
            # 提取dt的值（处理单分区和多分区情况）
            if partition_str.startswith("dt="):
                dt_value = partition_str.split("dt=")[1].split("/")[0]
                dt_partitions.append(dt_value)
        # 去重并返回
        return list(set(dt_partitions))
    except Exception as e:
        print(f"[WARN] 获取表{table_name}分区列表时出错: {str(e)}")
        return []


def process_table(spark, table_name, new_dt):
    """读取表中所有分区数据，仅修改dt为new_dt后新增回原表"""
    print(f"[INFO] 开始处理表：{table_name}，目标分区dt={new_dt}")

    # 检查目标分区是否已存在
    if check_partition_exists(spark, table_name, new_dt):
        print(f"[INFO] 目标分区dt={new_dt}已存在，跳过处理\n")
        return

    # 获取表中所有的dt分区
    dt_partitions = get_all_partitions(spark, table_name)
    if not dt_partitions:
        print(f"[WARN] 表{table_name}没有找到任何dt分区，跳过处理\n")
        return

    print(f"[INFO] 表{table_name}包含的dt分区: {', '.join(dt_partitions)}")

    # 读取所有分区数据（排除目标分区，避免重复）
    try:
        # 构建分区过滤条件
        if len(dt_partitions) == 1:
            where_clause = f"dt = '{dt_partitions[0]}'"
        else:
            where_clause = "dt IN ('{}')".format("', '".join(dt_partitions))

        df = spark.sql(f"SELECT * FROM {table_name} WHERE {where_clause}")
    except Exception as e:
        print(f"[ERROR] 读取表{table_name}数据失败: {str(e)}")
        return

    if df.count() == 0:
        print(f"[INFO] 表{table_name}所有分区无数据，跳过处理\n")
        return

    # 只替换dt字段为新日期，其他字段保持不变
    df_new = df.withColumn("dt", lit(new_dt))

    # 使用insertInto写入，自动匹配表结构和分区
    df_new.write.mode("append").insertInto(table_name)
    print(f"[INFO] 表{table_name}处理完成，新增分区dt={new_dt}，数据量：{df_new.count()}行\n")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="从Hive读取数据，修改dt分区后新增回原表")
    parser.add_argument(
        "--new_dt",
        type=str,
        default="2025-07-10",
        help="目标分区日期（如2025-07-10）"
    )
    args = parser.parse_args()
    new_dt = args.new_dt

    # 初始化Spark
    spark = get_spark_session()
    print(f"[INFO] 启动Spark会话，目标分区dt={new_dt}\n")

    # 定义需要处理的表列表
    tables_to_process = [
        "dwd.dwd_trade_cart_add_inc",
        "dwd.dwd_trade_order_detail_inc",
        "dwd.dwd_trade_pay_detail_suc_inc",
        "dwd.dwd_trade_cart_full",
        "dwd.dwd_trade_trade_flow_acc",
        "dwd.dwd_tool_coupon_used_inc",
        "dwd.dwd_interaction_favor_add_inc",
        "dwd.dwd_traffic_page_view_inc",
        "dwd.dwd_user_register_inc",
        "dwd.dwd_user_login_inc"
    ]

    # 批量处理所有表
    for table in tables_to_process:
        process_table(spark, table, new_dt)

    # 关闭Spark会话
    spark.stop()
    print(f"[INFO] 所有表处理完成，已为每个表新增分区dt={new_dt}")


if __name__ == "__main__":
    main()