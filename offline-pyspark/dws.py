from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import lit

# 目标日期
TARGET_DT = "2025-07-10"


def get_spark_session():
    """创建连接到CDH01 Hive Metastore的Spark会话"""
    try:
        spark = SparkSession.builder \
            .appName("DWS_Data_Duplicator") \
            .enableHiveSupport() \
            .config("hive.metastore.uris", "thrift://cdh01:9083") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.sql.debug.maxToStringFields", "1000") \
            .getOrCreate()
        print("✅ Spark会话已成功创建，已连接到CDH01的Hive")
        return spark
    except Exception as e:
        print(f"❌ 创建Spark会话失败: {str(e)}")
        sys.exit(1)


def list_dws_tables(spark):
    """列出dws库中的所有表"""
    try:
        spark.sql("USE dws")
        tables_df = spark.sql("SHOW TABLES")
        tables = [row.tableName for row in tables_df.collect()]
        print(f"\n📊 dws层共发现 {len(tables)} 张表:")
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table}")
        return tables
    except Exception as e:
        print(f"❌ 获取dws层表列表失败: {str(e)}")
        return []


def copy_table_with_new_dt(spark, table_name):
    """复制表数据并将dt字段修改为目标日期，使用Hive SQL插入"""
    try:
        print(f"\n===== 处理表: dws.{table_name} =====")

        # 1. 检查是否有dt字段
        df_original = spark.table(f"dws.{table_name}")
        if "dt" not in df_original.columns:
            print(f"⚠️ 表 {table_name} 不包含dt字段，跳过处理")
            return "skipped"

        # 2. 获取原始数据量
        original_count = df_original.count()
        print(f"📥 原始数据量: {original_count} 行")

        # 3. 构建不包含dt的字段列表（解决列数不匹配问题）
        non_partition_columns = [col for col in df_original.columns if col != 'dt']
        columns_str = ', '.join(non_partition_columns)

        # 4. 使用Hive SQL插入数据（分区字段在PARTITION中指定，SELECT不包含dt）
        print(f"\n🚧 正在通过Hive SQL插入数据到dt={TARGET_DT} 分区...")
        insert_query = f"""
            INSERT INTO TABLE dws.{table_name} PARTITION (dt='{TARGET_DT}')
            SELECT {columns_str}
            FROM dws.{table_name}
        """
        spark.sql(insert_query)

        # 5. 验证写入结果
        new_partition_df = spark.table(f"dws.{table_name}").where(f"dt = '{TARGET_DT}'")
        new_partition_count = new_partition_df.count()

        print(f"\n✅ 复制完成！")
        print(f"   新分区 dt={TARGET_DT} 的数据量: {new_partition_count} 行")

        if new_partition_count == original_count:
            print("   ✅ 新分区数据量与原始数据量一致")
            return "success"
        else:
            print(f"   ⚠️ 新分区数据量与原始数据量不一致（原始: {original_count}, 新分区: {new_partition_count}）")
            return "mismatch"

    except Exception as e:
        print(f"❌ 处理表 {table_name} 失败: {str(e)}")
        return "failed"


def main():
    # 1. 创建Spark会话
    spark = get_spark_session()
    print(f"🎯 目标日期: dt={TARGET_DT}")

    # 2. 获取dws层表列表
    tables = list_dws_tables(spark)
    if not tables:
        spark.stop()
        return

    # 3. 批量处理所有表
    processed = 0
    skipped = 0
    failed = 0
    mismatch = 0

    for i, table in enumerate(tables, 1):
        print(f"\n({i}/{len(tables)}) 正在处理表: {table}")
        result = copy_table_with_new_dt(spark, table)
        if result == "success":
            processed += 1
        elif result == "skipped":
            skipped += 1
        elif result == "failed":
            failed += 1
        elif result == "mismatch":
            processed += 1
            mismatch += 1

    # 4. 输出处理结果
    print(f"\n===== 处理完成 =====")
    print(f"  成功处理: {processed} 张表")
    print(f"  数据量不匹配: {mismatch} 张表")
    print(f"  跳过: {skipped} 张表（无dt字段）")
    print(f"  失败: {failed} 张表")

    # 5. 关闭会话
    spark.stop()
    print("\n✅ Spark会话已关闭")


if __name__ == "__main__":
    main()