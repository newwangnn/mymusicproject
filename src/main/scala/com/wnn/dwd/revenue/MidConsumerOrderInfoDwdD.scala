package com.wnn.dwd.revenue

import com.wnn.common.GenerateDate
import org.apache.spark.sql.SparkSession

object MidConsumerOrderInfoDwdD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("过滤每日消费订单信息")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions", 5)
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic ")


    session.sql(
      """
        |
        |create external table if not exists TWD_revenue_machineconsumedetail_d(
        |id int ,
        |trade_no string,
        |uid int ,
        |mid int,
        |p_type int, --1Kshow，2Minik，3Kshow之王街机版，4乐方，8MiniShow
        |m_type int, --0投币，1K金币，2银币，3音乐积分，4微信，5支付宝，6免费券，7QQ，9招行一网通
        |action_time string,
        |pkg_id int,
        |pkg_name string,
        |coin_price decimal(10,2), --单位：分
        |coin_CNT int,
        |order_id string,
        |activity_name string, --空没有参加活动
        |pkg_price  decimal(10,2),
        |pkg_discount  decimal(10,2),
        |coupon_type int,
        |order_Type int  -- 1 正常订单无异常,  2 商家退款,3 异常订单
        |)
        |partitioned by (dt_data string)
        |row format delimited
        |fields terminated by '\t'
        |location "hdfs://mycluster/mymusic/hive/dwd/revenue/TWD_revenue_machineconsumedetail_d"
      """.stripMargin)

    //只是将ods源消费记录处理：将退款为负数改成正数，订单类型重新定义
    session.sql(
      """
        |SELECT
        |id,
        |trade_no ,
        |uid ,
        |mid ,
        |p_type ,
        |m_type ,
        |action_time ,
        |pkg_id ,
        |pkg_name ,
        |case when amount <0 then amount*-1 else amount end coin_price,
        |1 as coin_CNT,
        |order_id ,
        |activity_name ,
        |pkg_price ,
        |pkg_discount ,
        |coupon_type ,
        |CASE WHEN order_type =1 then 1 when order_type =2 then 3 else 2 end orderType
        |from to_revenue_machineconsumedetail_d
      """.stripMargin).createTempView("temp_midconsumerorder")

    session.sql(
      s"""
        | insert into table TWD_revenue_machineconsumedetail_d partition(dt_data=${GenerateDate.dateToString()}) select * from temp_midconsumerorder
      """.stripMargin)


  }

}
