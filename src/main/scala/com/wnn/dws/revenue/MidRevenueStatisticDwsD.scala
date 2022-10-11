package com.wnn.dws.revenue

import com.wnn.common.GenerateDate
import org.apache.spark.sql.SparkSession

object MidRevenueStatisticDwsD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("分析每台机器每日营收统计")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic")

    session.sql(
      """
        | select
        | mid,pkg_id,m_type,
        | count(distinct uid ) total_user_cnt,
        | count(order_id) total_order_cnt,
        | sum(coin_price * coin_cnt) total_revenue
        | from  twd_revenue_machineconsumedetail_d
        |  where order_type=1
        | group by mid,pkg_id,m_type
      """.stripMargin).createTempView("temp_revenue_rev")

    session.sql(
      """
        | select
        | mid,pkg_id,m_type,
        | count(distinct uid ) total_user_refund_cnt,
        | count(order_id) total_order_refund_cnt,
        | sum(coin_price* coin_cnt) total_refund
        | from  twd_revenue_machineconsumedetail_d
        | where order_type=2 and order_id in (select order_id from twd_revenue_machineconsumedetail_d where order_type=1)
        | group by mid,pkg_id,m_type
      """.stripMargin).createTempView("temp_revenue_refund")

    session.sql(
      """
        | select
        | reg_mid as mid,
        | count(distinct uid) new_user_cnt
        | from tw_user_baseinfo_d
        | group by reg_mid
      """.stripMargin).createTempView("temp_mac_user")


    session.sql(
      """
        | select * from ( select
        | od.mid,od.p_type,od.pkg_id,
        |  case when od.m_type=0 then "投币"
        |     when od.m_type=1 then "K金币"
        |     when od.m_type=2 then "银币"
        |     when od.m_type=3 then "音乐积分"
        |     when od.m_type=4 then "微信"
        |     when od.m_type=5 then "支付宝"
        |     when od.m_type=6 then "免费券"
        |     when od.m_type=7 then "QQ" end  as pay_type ,
        | mb.machine_name,
        | mb.store_address,
        | mb.package_num ,
        | mb.package_name,
        | mb.pay_switch,
        | mb.sub_scence_cate_name,
        | mb.sub_scence_name,
        | mb.brand_name      ,
        | mb.sub_brand_name  ,
        | mb.adminid ,
        | mb.inv_rate ,
        | mb.age_rate ,
        | mb.com_rate ,
        | mb.par_rate ,
        | NVL(  ml.province,mb.province ) province,
        | NVL(ml.city,mb.city ) city,
        | NVL(ml.township,mb.address ) township ,
        | ml.addressdetail,
        | rev.total_user_cnt,rev.total_order_cnt,rev.total_revenue,
        | ref.total_user_refund_cnt,ref.total_order_refund_cnt,ref.total_refund,
        | user.new_user_cnt
        | from twd_revenue_machineconsumedetail_d od
        | left join temp_revenue_rev  rev
        | on  od.mid = rev.mid
        | left join temp_revenue_refund  ref
        | on  od.mid = ref.mid
        | left join temp_mac_user user
        | on od.mid = user.mid
        | left join tw_mac_baseinfo_d mb
        | on od.mid = mb.mid
        | left join twd_revenue_machineactuallocation_d ml
        | on od.mid =ml.mid
        | )a
        | where a.machine_name is not NULL and a.inv_rate is not NULL
        |
        |
      """.stripMargin)
      .createTempView("temp_revenue_macstatistic")

    session.sql(
      """
        | create external table if not exists TWS_revenue_MacStatistic_d(
        |mid  	int,
        |p_type int,
        |pkg_id int,
        |m_type String,
        |machine_name  string,
        |store_address string,
        |package_num int,
        |package_name string,
        |pay_switch string,
        |sub_scence_cate_name string,
        |sub_scence_name      string,
        |brand_name           string,
        |sub_brand_name       string,
        |adminid int,
        |inv_rate   decimal(10,2),
        |age_rate decimal(10,2),
        |com_rate decimal(10,2),
        |par_rate	decimal(10,2),
        |province       string,
        |city           string,
        |township       string,
        |addressdetail  string,
        |total_user_cnt			int,
        |total_order_cnt         int,
        |total_revenue           decimal(10,2),
        |total_user_refund_cnt   int,
        |total_order_refund_cnt  int,
        |total_refund            decimal(10,2),
        |new_user_cnt            int
        |)
        |partitioned by (dt_data string)
        |row format delimited
        |fields terminated by '\t'
        |location "hdfs://mycluster/mymusic/hive/dws/revenue/TWS_revenue_MacStatistic_d"
      """.stripMargin)


    session.sql(
      s"""
        |   insert overwrite TWS_revenue_MacStatistic_d partition (dt_data=${GenerateDate.dateToString()}) select * from temp_revenue_macstatistic
      """.stripMargin )

  }

}
