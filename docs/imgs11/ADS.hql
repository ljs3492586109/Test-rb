create database tms_ads
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/';
set hive.exec.mode.local.auto=True;
use tms;


drop table if exists tms_ads.ads_trans_order_stats;
create external table tms_ads.ads_trans_order_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    receive_order_count bigint COMMENT '接单总数',
    receive_order_amount decimal(16,2) COMMENT '接单金额',
    dispatch_order_count bigint COMMENT '发单总数',
    dispatch_order_amount decimal(16,2) COMMENT '发单金额'
) comment '运单相关统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_trans_order_stats';

insert overwrite table tms_ads.ads_trans_order_stats
select dt,
       recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from tms_ads.ads_trans_order_stats
union
select '2025-07-18'                                         dt,
       nvl(receive_1d.recent_days, dispatch_1d.recent_days) as recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select 1                 recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_1d
      where dt = '2025-07-18') receive_1d
         full outer join
     (select 1            recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from tms_dws.dws_trans_dispatch_1d
      where dt = '2025-07-18') dispatch_1d
     on receive_1d.recent_days = dispatch_1d.recent_days
union
select '2025-07-18'                                         dt,
       nvl(receive_nd.recent_days, dispatch_nd.recent_days)as recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_nd
      where dt = '2025-07-18'
      group by recent_days) receive_nd
         full outer join
     (select recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from tms_dws.dws_trans_dispatch_nd
      where dt = '2025-07-18') dispatch_nd
     on receive_nd.recent_days = dispatch_nd.recent_days;

select * from tms_ads.ads_trans_order_stats;


drop table if exists tms_ads.ads_trans_stats;
create external table tms_ads.ads_trans_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒'
) comment '运输综合统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_trans_stats';

insert overwrite table tms_ads.ads_trans_stats
select dt,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec
from tms_ads.ads_trans_stats
union
select '2025-07-18' as               dt,
       1                          recent_days,
       sum(trans_finish_count) as    trans_finish_count,
       sum(trans_finish_distance) as trans_finish_distance,
       sum(trans_finish_dur_sec) as  trans_finish_dur_sec
from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
where dt = '2025-07-18'
union
select '2025-07-18' as               dt,
       recent_days,
       sum(trans_finish_count) as    trans_finish_count,
       sum(trans_finish_distance) as trans_finish_distance,
       sum(trans_finish_dur_sec) as  trans_finish_dur_sec
from tms_dws.dws_trans_shift_trans_finish_nd
where dt = '2025-07-18'
group by recent_days;

select * from tms_ads.ads_trans_stats;


drop table if exists tms_ads.ads_trans_order_stats_td;
create external table tms_ads.ads_trans_order_stats_td(
    dt string COMMENT '统计日期',
    bounding_order_count bigint COMMENT '运输中运单总数',
    bounding_order_amount decimal(16,2) COMMENT '运输中运单金额'
) comment '历史至今运单统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_trans_order_stats_td';


insert overwrite table tms_ads.ads_trans_order_stats_td
select dt,
       bounding_order_count,
       bounding_order_amount
from tms_ads.ads_trans_order_stats_td
union
select dt,
       sum(order_count)  bounding_order_count,
       sum(order_amount) bounding_order_amount
from (select dt,
             order_count,
             order_amount
      from tms_dws.dws_trans_dispatch_td
      where dt = '2025-07-18'
      union
      select dt,
             order_count * (-1),
             order_amount * (-1)
      from tms_dws.dws_trans_bound_finish_td
      where dt = '2025-07-18') new
group by dt;

select * from tms_ads.ads_trans_order_stats_td;


drop table if exists tms_ads.ads_order_stats;
create external table tms_ads.ads_order_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    order_count bigint COMMENT '下单数',
    order_amount decimal(16,2) COMMENT '下单金额'
) comment '运单综合统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_order_stats';


insert overwrite table tms_ads.ads_order_stats
select dt,
       recent_days,
       order_count,
       order_amount
from tms_ads.ads_order_stats
union
select '2025-07-18' as      dt,
       1                 recent_days,
       sum(order_count) as  order_count,
       sum(order_amount) as order_amount
from tms_dws.dws_trade_org_cargo_type_order_1d
where dt = '2025-07-18'
union
select '2025-07-18' as      dt,
       recent_days,
       sum(order_count) as  order_count,
       sum(order_amount) as order_amount
from tms_dws.dws_trade_org_cargo_type_order_nd
where dt = '2025-07-18'
group by recent_days;

select * from tms_ads.ads_order_stats;


drop table if exists tms_ads.ads_order_cargo_type_stats;
create external table tms_ads.ads_order_cargo_type_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    cargo_type string COMMENT '货物类型',
    cargo_type_name string COMMENT '货物类型名称',
    order_count bigint COMMENT '下单数',
    order_amount decimal(16,2) COMMENT '下单金额'
) comment '各类型货物运单统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_order_cargo_type_stats';

insert overwrite table tms_ads.ads_order_cargo_type_stats
select dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from tms_ads.ads_order_cargo_type_stats
union
select '2025-07-18' as      dt,
       1                 recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count) as  order_count,
       sum(order_amount) as order_amount
from tms_dws.dws_trade_org_cargo_type_order_1d
where dt = '2025-07-18'
group by cargo_type,
         cargo_type_name
union
select '2025-07-18' as      dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count) as  order_count,
       sum(order_amount) as order_amount
from tms_dws.dws_trade_org_cargo_type_order_nd
where dt = '2025-07-18'
group by cargo_type,
         cargo_type_name,
         recent_days;

select * from tms_ads.ads_order_cargo_type_stats;


drop table if exists tms_ads.ads_city_stats;
create external table tms_ads.ads_city_stats(
    dt string COMMENT '统计日期',
    recent_days bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    city_id bigint COMMENT '城市ID',
    city_name string COMMENT '城市名称',
    order_count bigint COMMENT '下单数',
    order_amount decimal COMMENT '下单金额',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '城市分析'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_city_stats';

insert overwrite table tms_ads.ads_city_stats
select dt,
       recent_days,
       city_id,
       city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from tms_ads.ads_city_stats
union
select nvl(city_order_1d.dt, city_trans_1d.dt) as                   dt,
       nvl(city_order_1d.recent_days, city_trans_1d.recent_days) as recent_days,
       nvl(city_order_1d.city_id, city_trans_1d.city_id) as         city_id,
       nvl(city_order_1d.city_name, city_trans_1d.city_name)as     city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '2025-07-18'      dt,
             1                 recent_days,
             city_id,
             city_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from tms_dws.dws_trade_org_cargo_type_order_1d
      where dt = '2025-07-18'
      group by city_id,
               city_name) city_order_1d
         full outer join
     (select '2025-07-18'                                         dt,
             1                                                    recent_days,
             city_id,
             city_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from (select if(org_level = 1, city_for_level1.id, city_for_level2.id)     city_id,
                   if(org_level = 1, city_for_level1.name, city_for_level2.name) city_name,
                   trans_finish_count,
                   trans_finish_distance,
                   trans_finish_dur_sec
            from (select org_id,
                         trans_finish_count,
                         trans_finish_distance,
                         trans_finish_dur_sec
                  from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
                  where dt = '2025-07-18') trans_origin
                     left join
                 (select id,
                         org_level,
                         region_id
                  from tms_dim.dim_organ_full
                  where dt = '2025-07-18') organ
                 on org_id = organ.id
                     left join
                 (select id,
                         name,
                         parent_id
                  from tms_dim.dim_region_full
                  where dt = '2025-07-18') city_for_level1
                 on region_id = city_for_level1.id
                     left join
                 (select id,
                         name
                  from tms_dim.dim_region_full
                  where dt = '2025-07-18') city_for_level2
                 on city_for_level1.parent_id = city_for_level2.id) trans_1d
      group by city_id,
               city_name) city_trans_1d
     on city_order_1d.dt = city_trans_1d.dt
         and city_order_1d.recent_days = city_trans_1d.recent_days
         and city_order_1d.city_id = city_trans_1d.city_id
         and city_order_1d.city_name = city_trans_1d.city_name
union
select nvl(city_order_nd.dt, city_trans_nd.dt)  as                  dt,
       nvl(city_order_nd.recent_days, city_trans_nd.recent_days) as recent_days,
       nvl(city_order_nd.city_id, city_trans_nd.city_id)as         city_id,
       nvl(city_order_nd.city_name, city_trans_nd.city_name)as     city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '2025-07-18'      dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from tms_dws.dws_trade_org_cargo_type_order_nd
      where dt = '2025-07-18'
      group by city_id,
               city_name,
               recent_days) city_order_nd
         full outer join
     (select '2025-07-18'                                         dt,
             city_id,
             city_name,
             recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from tms_dws.dws_trans_shift_trans_finish_nd
      where dt = '2025-07-18'
      group by city_id,
               city_name,
               recent_days
     ) city_trans_nd
     on city_order_nd.dt = city_trans_nd.dt
         and city_order_nd.recent_days = city_trans_nd.recent_days
         and city_order_nd.city_id = city_trans_nd.city_id
         and city_order_nd.city_name = city_trans_nd.city_name;

select * from tms_ads.ads_city_stats;


drop table if exists tms_ads.ads_org_stats;
create external table tms_ads.ads_org_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    org_id bigint COMMENT '机构ID',
    org_name string COMMENT '机构名称',
    order_count bigint COMMENT '下单数',
    order_amount decimal COMMENT '下单金额',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '机构分析'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_org_stats';


insert overwrite table tms_ads.ads_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from tms_ads.ads_org_stats
union
select nvl(org_order_1d.dt, org_trans_1d.dt)as                   dt,
       nvl(org_order_1d.recent_days, org_trans_1d.recent_days) as recent_days,
       nvl(org_order_1d.org_id, org_trans_1d.org_id)as           org_id,
       nvl(org_order_1d.org_name, org_trans_1d.org_name)as       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '2025-07-18'      dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from tms_dws.dws_trade_org_cargo_type_order_1d
      where dt = '2025-07-18'
      group by org_id,
               org_name) org_order_1d
         full outer join
     (select '2025-07-18'                                         dt,
             org_id,
             org_name,
             1                                                    recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
      where dt = '2025-07-18'
      group by org_id,
               org_name
     ) org_trans_1d
     on org_order_1d.dt = org_trans_1d.dt
         and org_order_1d.recent_days = org_trans_1d.recent_days
         and org_order_1d.org_id = org_trans_1d.org_id
         and org_order_1d.org_name = org_trans_1d.org_name
union
select org_order_nd.dt,
       org_order_nd.recent_days,
       org_order_nd.org_id,
       org_order_nd.org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '2025-07-18'      dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from tms_dws.dws_trade_org_cargo_type_order_nd
      where dt = '2025-07-18'
      group by org_id,
               org_name,
               recent_days) org_order_nd
         join
     (select '2025-07-18'                                         dt,
             recent_days,
             org_id,
             org_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from tms_dws.dws_trans_shift_trans_finish_nd
      where dt = '2025-07-18'
      group by org_id,
               org_name,
               recent_days
     ) org_trans_nd
     on org_order_nd.dt = org_trans_nd.dt
         and org_order_nd.recent_days = org_trans_nd.recent_days
         and org_order_nd.org_id = org_trans_nd.org_id
         and org_order_nd.org_name = org_trans_nd.org_name;


select * from tms_ads.ads_org_stats;

drop table if exists tms_ads.ads_shift_stats;
create external table tms_ads.ads_shift_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    shift_id bigint COMMENT '班次ID',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    trans_finish_order_count bigint COMMENT '运输完成运单数'
) comment '班次分析'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_shift_stats';


insert overwrite table tms_ads.ads_shift_stats
select dt,
       recent_days,
       shift_id,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from tms_ads.ads_shift_stats
union
select '2025-07-18' as dt,
       recent_days,
       shift_id,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from tms_dws.dws_trans_shift_trans_finish_nd
where dt = '2025-07-18';

select * from tms_ads.ads_shift_stats;

drop table if exists tms_ads.ads_line_stats;
create external table tms_ads.ads_line_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    line_id bigint COMMENT '线路ID',
    line_name string COMMENT '线路名称',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    trans_finish_order_count bigint COMMENT '运输完成运单数'
) comment '线路分析'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_line_stats';


insert overwrite table tms_ads.ads_line_stats
select dt,
       recent_days,
       line_id,
       line_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from tms_ads.ads_line_stats
union
select '2025-07-18' as                  dt,
       recent_days,
       line_id,
       line_name,
       sum(trans_finish_count)as       trans_finish_count,
       sum(trans_finish_distance)as    trans_finish_distance,
       sum(trans_finish_dur_sec)as     trans_finish_dur_sec,
       sum(trans_finish_order_count)as trans_finish_order_count
from tms_dws.dws_trans_shift_trans_finish_nd
where dt = '2025-07-18'
group by line_id,
         line_name,
         recent_days;

select * from tms_ads.ads_line_stats;


drop table if exists tms_ads.ads_driver_stats;
create external table tms_ads.ads_driver_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    driver_emp_id bigint comment '第一司机员工ID',
    driver_name string comment '第一司机姓名',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '司机分析'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_driver_stats';


insert overwrite table tms_ads.ads_driver_stats
select dt,
       recent_days,
       driver_emp_id,
       driver_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from tms_ads.ads_driver_stats
union
select '2025-07-18'                                         dt,
       recent_days,
       driver_id,
       driver_name,
       sum(trans_finish_count)  as                             trans_finish_count,
       sum(trans_finish_distance)  as                          trans_finish_distance,
       sum(trans_finish_dur_sec)    as                         trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from (select recent_days,
             driver1_emp_id driver_id,
             driver1_name   driver_name,
             trans_finish_count,
             trans_finish_distance,
             trans_finish_dur_sec
      from tms_dws.dws_trans_shift_trans_finish_nd
      where dt = '2025-07-18'
        and driver2_emp_id is null
      union
      select recent_days,
             cast(driver_info[0] as bigint) driver_id,
             driver_info[1] driver_name,
             trans_finish_count,
             trans_finish_distance,
             trans_finish_dur_sec
      from (select recent_days,
                   array(array(driver1_emp_id, driver1_name),
                         array(driver2_emp_id, driver2_name)) driver_arr,
                   trans_finish_count,
                   trans_finish_distance / 2                  trans_finish_distance,
                   trans_finish_dur_sec / 2                   trans_finish_dur_sec
            from tms_dws.dws_trans_shift_trans_finish_nd
            where dt = '2025-07-18'
              and driver2_emp_id is not null) t1
               lateral view explode(driver_arr) tmp as driver_info) t2
group by driver_id,
         driver_name,
         recent_days;

select * from tms_ads.ads_driver_stats;


drop table if exists tms_ads.ads_truck_stats;
create external table tms_ads.ads_truck_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    truck_model_type string COMMENT '卡车类别编码',
    truck_model_type_name string COMMENT '卡车类别名称',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '卡车分析'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_truck_stats';


insert overwrite table tms_ads.ads_truck_stats
select dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from tms_ads.ads_truck_stats
union
select '2025-07-18' as                                         dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       sum(trans_finish_count)  as                            trans_finish_count,
       sum(trans_finish_distance)  as                         trans_finish_distance,
       sum(trans_finish_dur_sec)    as                        trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from tms_dws.dws_trans_shift_trans_finish_nd
where dt = '2025-07-18'
group by truck_model_type,
         truck_model_type_name,
         recent_days;

select * from tms_ads.ads_truck_stats;


drop table if exists tms_ads.ads_express_stats;
create external table tms_ads.ads_express_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    deliver_suc_count bigint COMMENT '派送成功次数（订单数）',
    sort_count bigint COMMENT '分拣次数'
) comment '快递综合统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_express_stats';

insert overwrite table tms_ads.ads_express_stats
select dt,
       recent_days,
       deliver_suc_count,
       sort_count
from tms_ads.ads_express_stats
union
select nvl(deliver_1d.dt, sort_1d.dt) as                  dt,
       nvl(deliver_1d.recent_days, sort_1d.recent_days)as recent_days,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             1                recent_days,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_1d
      where dt = '2025-07-18') deliver_1d
         full outer join
     (select '2025-07-18'    dt,
             1               recent_days,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_1d
      where dt = '2025-07-18') sort_1d
     on deliver_1d.dt = sort_1d.dt
         and deliver_1d.recent_days = sort_1d.recent_days
union
select nvl(deliver_nd.dt, sort_nd.dt) as                  dt,
       nvl(deliver_nd.recent_days, sort_nd.recent_days)as recent_days,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             recent_days,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_nd
      where dt = '2025-07-18'
      group by recent_days) deliver_nd
         full outer join
     (select '2025-07-18'    dt,
             recent_days,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_nd
      where dt = '2025-07-18'
      group by recent_days) sort_nd
     on deliver_nd.dt = sort_nd.dt
         and deliver_nd.recent_days = sort_nd.recent_days;

select * from tms_ads.ads_express_stats;


drop table if exists tms_ads.ads_express_province_stats;
create external table tms_ads.ads_express_province_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    province_id bigint COMMENT '省份ID',
    province_name string COMMENT '省份名称',
    receive_order_count bigint COMMENT '揽收次数',
    receive_order_amount decimal(16,2) COMMENT '揽收金额',
    deliver_suc_count bigint COMMENT '派送成功次数',
    sort_count bigint COMMENT '分拣次数'
) comment '各省份快递统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_express_province_stats';


insert overwrite table tms_ads.ads_express_province_stats
select dt,
       recent_days,
       province_id,
       province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from tms_ads.ads_express_province_stats
union
select nvl(nvl(province_deliver_1d.dt, province_sort_1d.dt), province_receive_1d.dt)as dt,
       nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days),
           province_receive_1d.recent_days)as   recent_days,
       nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id),
           province_receive_1d.province_id) as   province_id,
       nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name),
           province_receive_1d.province_name) as    province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             1                recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_1d
      where dt = '2025-07-18'
      group by province_id,
               province_name) province_deliver_1d
         full outer join
     (select '2025-07-18'    dt,
             1               recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_1d
      where dt = '2025-07-18'
      group by province_id,
               province_name) province_sort_1d
     on province_deliver_1d.dt = province_sort_1d.dt
         and province_deliver_1d.recent_days = province_sort_1d.recent_days
         and province_deliver_1d.province_id = province_sort_1d.province_id
         and province_deliver_1d.province_name = province_sort_1d.province_name
         full outer join
     (select '2025-07-18'      dt,
             1                 recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_1d
      where dt = '2025-07-18'
      group by province_id,
               province_name) province_receive_1d
     on province_deliver_1d.dt = province_receive_1d.dt
         and province_deliver_1d.recent_days = province_receive_1d.recent_days
         and province_deliver_1d.province_id = province_receive_1d.province_id
         and province_deliver_1d.province_name = province_receive_1d.province_name
union
select nvl(nvl(province_deliver_nd.dt, province_sort_nd.dt), province_receive_nd.dt)as dt,
       nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days),
           province_receive_nd.recent_days) as                                         recent_days,
       nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id),
           province_receive_nd.province_id)    as                        province_id,
       nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name),
           province_receive_nd.province_name)   as                                     province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_nd
      where dt = '2025-07-18'
      group by recent_days,
               province_id,
               province_name) province_deliver_nd
         full outer join
     (select '2025-07-18'    dt,
             recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_nd
      where dt = '2025-07-18'
      group by recent_days,
               province_id,
               province_name) province_sort_nd
     on province_deliver_nd.dt = province_sort_nd.dt
         and province_deliver_nd.recent_days = province_sort_nd.recent_days
         and province_deliver_nd.province_id = province_sort_nd.province_id
         and province_deliver_nd.province_name = province_sort_nd.province_name
         full outer join
     (select '2025-07-18'      dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_nd
      where dt = '2025-07-18'
      group by recent_days,
               province_id,
               province_name) province_receive_nd
     on province_deliver_nd.dt = province_receive_nd.dt
         and province_deliver_nd.recent_days = province_receive_nd.recent_days
         and province_deliver_nd.province_id = province_receive_nd.province_id
         and province_deliver_nd.province_name = province_receive_nd.province_name;


select * from tms_ads.ads_express_province_stats;

drop table if exists tms_ads.ads_express_city_stats;
create external table tms_ads.ads_express_city_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    city_id bigint COMMENT '城市ID',
    city_name string COMMENT '城市名称',
    receive_order_count bigint COMMENT '揽收次数',
    receive_order_amount decimal(16,2) COMMENT '揽收金额',
    deliver_suc_count bigint COMMENT '派送成功次数',
    sort_count bigint COMMENT '分拣次数'
) comment '各城市快递统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_express_city_stats';


insert overwrite table tms_ads.ads_express_city_stats
select dt,
       recent_days,
       city_id,
       city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from tms_ads.ads_express_city_stats
union
select nvl(nvl(city_deliver_1d.dt, city_sort_1d.dt), city_receive_1d.dt) as dt,
       nvl(nvl(city_deliver_1d.recent_days, city_sort_1d.recent_days),
           city_receive_1d.recent_days)  as                                recent_days,
       nvl(nvl(city_deliver_1d.city_id, city_sort_1d.city_id),
           city_receive_1d.city_id)      as                                city_id,
       nvl(nvl(city_deliver_1d.city_name, city_sort_1d.city_name),
           city_receive_1d.city_name)   as                                 city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             1                recent_days,
             city_id,
             city_name,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_1d
      where dt = '2025-07-18'
      group by city_id,
               city_name) city_deliver_1d
         full outer join
     (select '2025-07-18'    dt,
             1               recent_days,
             city_id,
             city_name,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_1d
      where dt = '2025-07-18'
      group by city_id,
               city_name) city_sort_1d
     on city_deliver_1d.dt = city_sort_1d.dt
         and city_deliver_1d.recent_days = city_sort_1d.recent_days
         and city_deliver_1d.city_id = city_sort_1d.city_id
         and city_deliver_1d.city_name = city_sort_1d.city_name
         full outer join
     (select '2025-07-18'      dt,
             1                 recent_days,
             city_id,
             city_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_1d
      where dt = '2025-07-18'
      group by city_id,
               city_name) city_receive_1d
     on city_deliver_1d.dt = city_receive_1d.dt
         and city_deliver_1d.recent_days = city_receive_1d.recent_days
         and city_deliver_1d.city_id = city_receive_1d.city_id
         and city_deliver_1d.city_name = city_receive_1d.city_name
union
select nvl(nvl(city_deliver_nd.dt, city_sort_nd.dt), city_receive_nd.dt) as dt,
       nvl(nvl(city_deliver_nd.recent_days, city_sort_nd.recent_days),
           city_receive_nd.recent_days) as                                 recent_days,
       nvl(nvl(city_deliver_nd.city_id, city_sort_nd.city_id),
           city_receive_nd.city_id)    as                                  city_id,
       nvl(nvl(city_deliver_nd.city_name, city_sort_nd.city_name),
           city_receive_nd.city_name)  as                                  city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_nd
      where dt = '2025-07-18'
      group by recent_days,
               city_id,
               city_name) city_deliver_nd
         full outer join
     (select '2025-07-18'    dt,
             recent_days,
             city_id,
             city_name,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_nd
      where dt = '2025-07-18'
      group by recent_days,
               city_id,
               city_name) city_sort_nd
     on city_deliver_nd.dt = city_sort_nd.dt
         and city_deliver_nd.recent_days = city_sort_nd.recent_days
         and city_deliver_nd.city_id = city_sort_nd.city_id
         and city_deliver_nd.city_name = city_sort_nd.city_name
         full outer join
     (select '2025-07-18'      dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_nd
      where dt = '2025-07-18'
      group by recent_days,
               city_id,
               city_name) city_receive_nd
     on city_deliver_nd.dt = city_receive_nd.dt
         and city_deliver_nd.recent_days = city_receive_nd.recent_days
         and city_deliver_nd.city_id = city_receive_nd.city_id
         and city_deliver_nd.city_name = city_receive_nd.city_name;

select * from tms_ads.ads_express_city_stats;

drop table if exists tms_ads.ads_express_org_stats;
create external table tms_ads.ads_express_org_stats(
    dt string COMMENT '统计日期',
    recent_days tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    org_id bigint COMMENT '机构ID',
    org_name string COMMENT '机构名称',
    receive_order_count bigint COMMENT '揽收次数',
    receive_order_amount decimal(16,2) COMMENT '揽收金额',
    deliver_suc_count bigint COMMENT '派送成功次数',
    sort_count bigint COMMENT '分拣次数'
) comment '各机构快递统计'
    row format delimited fields terminated by '\t'
    location '/bigdata_warehouse/tms/tms_ads/tms_ads.ads_express_org_stats';


insert overwrite table tms_ads.ads_express_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from tms_ads.ads_express_org_stats
union
select nvl(nvl(org_deliver_1d.dt, org_sort_1d.dt), org_receive_1d.dt) as dt,
       nvl(nvl(org_deliver_1d.recent_days, org_sort_1d.recent_days),
           org_receive_1d.recent_days)  as                              recent_days,
       nvl(nvl(org_deliver_1d.org_id, org_sort_1d.org_id),
           org_receive_1d.org_id) as                                    org_id,
       nvl(nvl(org_deliver_1d.org_name, org_sort_1d.org_name),
           org_receive_1d.org_name)  as                                 org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             1                recent_days,
             org_id,
             org_name,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_1d
      where dt = '2025-07-18'
      group by org_id,
               org_name) org_deliver_1d
         full outer join
     (select '2025-07-18'    dt,
             1               recent_days,
             org_id,
             org_name,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_1d
      where dt = '2025-07-18'
      group by org_id,
               org_name) org_sort_1d
     on org_deliver_1d.dt = org_sort_1d.dt
         and org_deliver_1d.recent_days = org_sort_1d.recent_days
         and org_deliver_1d.org_id = org_sort_1d.org_id
         and org_deliver_1d.org_name = org_sort_1d.org_name
         full outer join
     (select '2025-07-18'      dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_1d
      where dt = '2025-07-18'
      group by org_id,
               org_name) org_receive_1d
     on org_deliver_1d.dt = org_receive_1d.dt
         and org_deliver_1d.recent_days = org_receive_1d.recent_days
         and org_deliver_1d.org_id = org_receive_1d.org_id
         and org_deliver_1d.org_name = org_receive_1d.org_name
union
select nvl(nvl(org_deliver_nd.dt, org_sort_nd.dt), org_receive_nd.dt) as dt,
       nvl(nvl(org_deliver_nd.recent_days, org_sort_nd.recent_days),
           org_receive_nd.recent_days)               as                 recent_days,
       nvl(nvl(org_deliver_nd.org_id, org_sort_nd.org_id),
           org_receive_nd.org_id)                 as                    org_id,
       nvl(nvl(org_deliver_nd.org_name, org_sort_nd.org_name),
           org_receive_nd.org_name)           as                        org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2025-07-18'     dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count) deliver_suc_count
      from tms_dws.dws_trans_org_deliver_suc_nd
      where dt = '2025-07-18'
      group by recent_days,
               org_id,
               org_name) org_deliver_nd
         full outer join
     (select '2025-07-18'    dt,
             recent_days,
             org_id,
             org_name,
             sum(sort_count) sort_count
      from tms_dws.dws_trans_org_sort_nd
      where dt = '2025-07-18'
      group by recent_days,
               org_id,
               org_name) org_sort_nd
     on org_deliver_nd.dt = org_sort_nd.dt
         and org_deliver_nd.recent_days = org_sort_nd.recent_days
         and org_deliver_nd.org_id = org_sort_nd.org_id
         and org_deliver_nd.org_name = org_sort_nd.org_name
         full outer join
     (select '2025-07-18'      dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from tms_dws.dws_trans_org_receive_nd
      where dt = '2025-07-18'
      group by recent_days,
               org_id,
               org_name) org_receive_nd
     on org_deliver_nd.dt = org_receive_nd.dt
         and org_deliver_nd.recent_days = org_receive_nd.recent_days
         and org_deliver_nd.org_id = org_receive_nd.org_id
         and org_deliver_nd.org_name = org_receive_nd.org_name;

select * from tms_ads.ads_express_org_stats;

