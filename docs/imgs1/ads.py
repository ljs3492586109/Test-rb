from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 创建SparkSession并启用Hive支持
spark = SparkSession.builder \
    .appName("HiveDataETL") \
    .config("hive.metastore.uris", "thrift://192.168.142.131:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# 需处理的表名列表
tables = [
    'ads_traffic_stats_by_channel',
    'ads_page_path',
    'ads_user_change',
    'ads_user_retention',
    'ads_user_stats',
    'ads_user_action',
    'ads_new_order_user_stats',
    'ads_order_continuously_user_count',
    'ads_repeat_purchase_by_tm',
    'ads_order_stats_by_tm',
    'ads_order_stats_by_cate',
    'ads_sku_cart_num_top3_by_cate',
    'ads_sku_favor_count_top3_by_tm',
    'ads_order_to_pay_interval_avg',
    'ads_order_by_province',
    'ads_coupon_stats'
]

# 遍历处理每个表
for table in tables:
    try:
        # 读取Hive表数据
        df = spark.table(f"ads.{table}")

        # 如果存在dt字段，则修改为2025-07-10
        if 'dt' in df.columns:
            df = df.withColumn('dt', lit('2025-07-10'))

        # 显式指定Hive文本格式和分隔符（与原表保持一致）
        df.write \
            .mode("append") \
            .format("hive") \
            .option("sep", "\t") \
            .option("serialization.format", "\t") \
            .saveAsTable(f"ads.{table}")

        print(f"表 {table} 处理成功")

    except Exception as e:
        print(f"表 {table} 处理失败: {str(e)}")

# 停止SparkSession
spark.stop()
print("所有表处理完毕")