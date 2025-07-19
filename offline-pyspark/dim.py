from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import lit


def get_spark_session():
    """初始化 SparkSession，启用 Hive 支持"""
    spark = SparkSession.builder \
        .appName("DimTableETL_Read20250701_Write20250710") \
        .config("hive.metastore.uris", "thrift://192.168.142.131:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE tms_dim")
    return spark


def check_partition_exists(spark, table_name, dt_col, dt_value):
    """Check if a partition already exists in the table"""
    try:
        count = spark.sql(f"""
            SELECT 1 FROM {table_name} 
            WHERE {dt_col} = '{dt_value}' 
            LIMIT 1
        """).count()
        return count > 0
    except:
        return False


def process_dim_sku_full(spark, read_dt, target_dt):
    """读取 dt=read_dt（2025-07-01），插入 dt=target_dt（2025-07-10）"""
    print(f"[INFO] 处理 dim_sku_full：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if partition already exists
    if check_partition_exists(spark, "dim.dim_sku_full", "dt", target_dt):
        print(f"[INFO] dim_sku_full 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        with
            sku as (
                select
                    id, price, sku_name, sku_desc, weight,
                    cast(is_sale as boolean) as is_sale,  -- 转换为 BOOLEAN 类型
                    spu_id, category3_id, tm_id, create_time
                from ods_sku_info
                where dt='{read_dt}'
            ),
            spu as (
                select id, spu_name from ods_spu_info where dt='{read_dt}'
            ),
            c3 as (
                select id, name, category2_id from ods_base_category3 where dt='{read_dt}'
            ),
            c2 as (
                select id, name, category1_id from ods_base_category2 where dt='{read_dt}'
            ),
            c1 as (
                select id, name from ods_base_category1 where dt='{read_dt}'
            ),
            tm as (
                select id, tm_name from ods_base_trademark where dt='{read_dt}'
            ),
            attr as (
                select
                    sku_id,
                    collect_set(
                        named_struct(
                            'attr_id', cast(attr_id as string),
                            'value_id', cast(value_id as string),
                            'attr_name', attr_name,
                            'value_name', value_name
                        )
                    ) as attrs
                from ods_sku_attr_value
                where dt='{read_dt}'
                group by sku_id
            ),
            sale_attr as (
                select
                    sku_id,
                    collect_set(
                        named_struct(
                            'sale_attr_id', cast(sale_attr_id as string),
                            'sale_attr_value_id', cast(sale_attr_value_id as string),
                            'sale_attr_name', sale_attr_name,
                            'sale_attr_value_name', sale_attr_value_name
                        )
                    ) as sale_attrs
                from ods_sku_sale_attr_value
                where dt='{read_dt}'
                group by sku_id
            )
        select
            sku.id, sku.price, sku.sku_name, sku.sku_desc, sku.weight, sku.is_sale,
            sku.spu_id, spu.spu_name, sku.category3_id, c3.name, c3.category2_id,
            c2.name, c2.category1_id, c1.name, sku.tm_id, tm.tm_name,
            attr.attrs, sale_attr.sale_attrs, sku.create_time,
            '{target_dt}' as dt
        from sku
            left join spu on sku.spu_id = spu.id
            left join c3 on sku.category3_id = c3.id
            left join c2 on c3.category2_id = c2.id
            left join c1 on c2.category1_id = c1.id
            left join tm on sku.tm_id = tm.id
            left join attr on sku.id = attr.sku_id
            left join sale_attr on sku.id = sale_attr.sku_id
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_sku_full")
    print(f"[INFO] dim_sku_full 处理完成，数据量：{df.count()} 行\n")


def process_dim_coupon_full(spark, read_dt, target_dt):
    """优惠券表：读取 2025-07-01，插入 2025-07-10"""
    print(f"[INFO] 处理 dim_coupon_full：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if partition already exists
    if check_partition_exists(spark, "dim.dim_coupon_full", "dt", target_dt):
        print(f"[INFO] dim_coupon_full 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        select
            id, coupon_name, coupon_type, coupon_dic.dic_name,
            condition_amount, condition_num, activity_id, benefit_amount,
            benefit_discount,
            case coupon_type
                when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
                when '3202' then concat('满', condition_num, '件打', benefit_discount, '折')
                when '3203' then concat('减', benefit_amount, '元')
            end benefit_rule,
            create_time, range_type, range_dic.dic_name,
            limit_num, taken_count, start_time, end_time, operate_time, expire_time,
            '{target_dt}' as dt
        from (
            select * from ods_coupon_info where dt='{read_dt}'
        ) ci
            left join (
                select dic_code, dic_name from ods_base_dic where dt='{read_dt}'
            ) coupon_dic on ci.coupon_type = coupon_dic.dic_code
            left join (
                select dic_code, dic_name from ods_base_dic where dt='{read_dt}'
            ) range_dic on ci.range_type = range_dic.dic_code
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_coupon_full")
    print(f"[INFO] dim_coupon_full 处理完成，数据量：{df.count()} 行\n")


def process_dim_activity_full(spark, read_dt, target_dt):
    """活动表：读取 2025-07-01，插入 2025-07-10"""
    print(f"[INFO] 处理 dim_activity_full：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if partition already exists
    if check_partition_exists(spark, "dim.dim_activity_full", "dt", target_dt):
        print(f"[INFO] dim_activity_full 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        select
            rule.id, info.id, activity_name, rule.activity_type, dic.dic_name,
            activity_desc, start_time, end_time, 
            info.create_time,  -- 明确指定使用info表的create_time
            condition_amount, condition_num, benefit_amount, benefit_discount,
            case rule.activity_type
                when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
                when '3102' then concat('满', condition_num, '件打', benefit_discount, '折')
                when '3103' then concat('打', benefit_discount, '折')
            end benefit_rule,
            benefit_level,
            '{target_dt}' as dt
        from (
            select * from ods_activity_rule where dt='{read_dt}'
        ) rule
            left join (
                select * from ods_activity_info where dt='{read_dt}'
            ) info on rule.activity_id = info.id
            left join (
                select dic_code, dic_name from ods_base_dic where dt='{read_dt}'
            ) dic on rule.activity_type = dic.dic_code
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_activity_full")
    print(f"[INFO] dim_activity_full 处理完成，数据量：{df.count()} 行\n")


def process_dim_province_full(spark, read_dt, target_dt):
    """地区表：读取 2025-07-01，插入 2025-07-10"""
    print(f"[INFO] 处理 dim_province_full：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if partition already exists
    if check_partition_exists(spark, "dim.dim_province_full", "dt", target_dt):
        print(f"[INFO] dim_province_full 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        select
            province.id, province.name, province.area_code, province.iso_code,
            province.iso_3166_2, region_id, region.region_name,
            '{target_dt}' as dt
        from (
            select * from ods_base_province where dt='{read_dt}'
        ) province
            left join (
                select id, region_name from ods_base_region where dt='{read_dt}'
            ) region on province.region_id = region.id
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_province_full")
    print(f"[INFO] dim_province_full 处理完成，数据量：{df.count()} 行\n")


def process_dim_promotion_pos_full(spark, read_dt, target_dt):
    """营销坑位表：读取 2025-07-01，插入 2025-07-10"""
    print(f"[INFO] 处理 dim_promotion_pos_full：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if partition already exists
    if check_partition_exists(spark, "dim.dim_promotion_pos_full", "dt", target_dt):
        print(f"[INFO] dim_promotion_pos_full 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        select id, pos_location, pos_type, promotion_type, create_time, operate_time,
               '{target_dt}' as dt
        from ods_promotion_pos where dt='{read_dt}'
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_promotion_pos_full")
    print(f"[INFO] dim_promotion_pos_full 处理完成，数据量：{df.count()} 行\n")


def process_dim_promotion_refer_full(spark, read_dt, target_dt):
    """营销渠道表：读取 2025-07-01，插入 2025-07-10"""
    print(f"[INFO] 处理 dim_promotion_refer_full：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if partition already exists
    if check_partition_exists(spark, "dim.dim_promotion_refer_full", "dt", target_dt):
        print(f"[INFO] dim_promotion_refer_full 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        select id, refer_name, create_time, operate_time,
               '{target_dt}' as dt
        from ods_promotion_refer where dt='{read_dt}'
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_promotion_refer_full")
    print(f"[INFO] dim_promotion_refer_full 处理完成，数据量：{df.count()} 行\n")


def process_dim_user_zip(spark, read_dt, target_dt):
    """用户拉链表：读取 2025-07-01，插入 2025-07-10"""
    print(f"[INFO] 处理 dim_user_zip：读取 {read_dt}，插入 {target_dt} 分区")

    # Check if we've already processed this target date
    if check_partition_exists(spark, "dim.dim_user_zip", "dt", target_dt):
        print(f"[INFO] dim_user_zip 分区 {target_dt} 已存在，跳过处理")
        return

    sql = f"""
        with merged_data as (
            -- 新增数据
            select
                cast(id as string) as id,
                concat(substr(name, 1, 1), '*') as name,
                case
                    when phone_num rlike '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$'
                    then concat(substr(phone_num, 1, 3), '****', substr(phone_num, 8))
                    else null
                end as phone_num,
                case
                    when email rlike '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$'
                    then concat('***@', split(email, '@')[1])
                    else null
                end as email,
                user_level, birthday, gender, create_time, operate_time,
                '{target_dt}' as start_date,
                '9999-12-31' as end_date,
                '{target_dt}' as dt
            from ods_user_info where dt='{read_dt}'

            union all

            -- 历史数据
            select
                id, name, phone_num, email, user_level, birthday, gender,
                create_time, operate_time, start_date, end_date,
                dt
            from dim.dim_user_zip where dt='{target_dt}'
        ),
        deduped_data as (
            select *,
                   row_number() over (partition by id order by start_date desc) as rn
            from merged_data
        )
        select
            id, name, phone_num, email, user_level, birthday, gender,
            create_time, operate_time, start_date,
            case when rn = 1 then '9999-12-31' else '{target_dt}' end as end_date,
            dt
        from deduped_data
    """

    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("dim.dim_user_zip")
    print(f"[INFO] dim_user_zip 处理完成，数据量：{df.count()} 行\n")


def process_dim_date(spark, target_dt):
    """日期表：无分区，直接插入数据"""
    print(f"[INFO] 处理 dim_date，直接插入数据（无分区）")

    # Check if table already has data
    if spark.table("dim.dim_date").count() > 0:
        print("[INFO] dim_date 表已有数据，跳过处理")
        return

    # 直接读取数据源并写入dim_date表，不涉及分区字段dt
    sql = f"""
        insert overwrite table dim.dim_date
        select * from tmp_dim_date_info  -- 假设tmp_dim_date_info是日期数据的临时表/源表
    """
    spark.sql(sql)
    # 统计dim_date表的总数据量（无分区过滤）
    count = spark.table("dim.dim_date").count()
    print(f"[INFO] dim_date 处理完成，总数据量：{count} 行\n")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="读取 dt=2025-07-01，插入指定目标分区")
    parser.add_argument(
        "--target_dt",
        type=str,
        default="2025-07-10",
        help="目标分区日期（如 2025-07-10）"
    )
    args = parser.parse_args()
    target_dt = args.target_dt
    read_dt = "2025-07-01"

    # 初始化 Spark
    spark = get_spark_session()
    print(f"[INFO] 开始 ETL：读取 {read_dt} 的源数据，插入 {target_dt} 的目标分区\n")

    # 批量处理所有表
    process_dim_sku_full(spark, read_dt, target_dt)
    process_dim_coupon_full(spark, read_dt, target_dt)
    process_dim_activity_full(spark, read_dt, target_dt)
    process_dim_province_full(spark, read_dt, target_dt)
    process_dim_promotion_pos_full(spark, read_dt, target_dt)
    process_dim_promotion_refer_full(spark, read_dt, target_dt)
    process_dim_user_zip(spark, read_dt, target_dt)
    process_dim_date(spark, target_dt)

    # 关闭 Spark 会话
    spark.stop()
    print(f"[INFO] 所有表处理完成：已将 {read_dt} 的数据插入目标表！")


if __name__ == "__main__":
    main()