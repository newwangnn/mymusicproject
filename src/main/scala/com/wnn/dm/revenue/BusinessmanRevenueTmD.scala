package com.wnn.dm.revenue

import java.util.Properties

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{SaveMode, SparkSession}

object BusinessmanRevenueTmD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("分析商户每日营收统计")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic")

    session.sql(
      """
        |
        |SELECT
        |a.*,
        | cast(totalRevenue*b.com_rate -totalRefund*b.com_rate  as decimal(20,2)),
        | cast(totalRevenue*b.par_rate -totalRefund*b.par_rate  as decimal(20,2)),
        | cast(totalRevenue*b.inv_rate-totalRefund*b.inv_rate   as decimal(20,2)),
        | cast(totalRevenue*b.age_rate -totalRefund*b.age_rate as decimal(20,2))
        |from (
        |select
        |adminid ,m_type as pay_type,
        |sum(nvl(total_order_cnt,0)) as totalOrderCnt,
        |sum(nvl(total_order_refund_cnt,0)) as totalOrderRefundCnt,
        |sum(nvl(total_revenue,0.0)) as totalRevenue,
        |sum(nvl(total_refund,0.0)) as totalRefund
        |from tws_revenue_macstatistic_d
        |group by adminid ,m_type
        |) a
        |left join tws_revenue_macstatistic_d b
        |on a.adminid =b.adminid
      """.stripMargin).createTempView("temp_tm_revenue_adminrevenue_d")


    session.sql(
      """
        | create EXTERNAL TABLE if not exists tm_revenue_adminrevenue_d(
        |adminid int,
        |pay_type string,
        |total_order_count int,
        |total_refund_order_count int,
        |total_revenue decimal(20,2),
        |total_refund_revenue decimal(20,2),
        |company decimal(20,2), --公司收入
        |partner  decimal(20,2),--合伙人收入
        |investor  decimal(20,2),--投资人收入
        |operator decimal(20,2) --运营人收入
        |)
        |partitioned by (dt_data string)
        |row format delimited
        |fields terminated by '\t'
        |location "hdfs://mycluster//mymusic/hive/tm/revenue/tm_revenue_adminrevenue_d"
        |
      """.stripMargin)


    session.sql(
      s"""
        | insert overwrite tm_revenue_adminrevenue_d partition(dt_data=${GenerateDate.dateToString()}) select * from temp_tm_revenue_adminrevenue_d
      """.stripMargin)

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("url","jdbc:mysql://hadoop52:3306/mymusicresult")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    val currentDate: String = GenerateDate.dateToString()

    session.sql(
      """
        | select *  from tm_revenue_adminrevenue_d
      """.stripMargin).write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop52:3306/mymusicresult","tm_revenue_adminrevenue_d",prop)
  }

}
