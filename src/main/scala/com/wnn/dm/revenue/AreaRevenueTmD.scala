package com.wnn.dm.revenue

import java.util.Properties

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaRevenueTmD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("统计省份每日营收")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic")

    session.sql(
      """
         |select
         |province,
         |sum(total_user_cnt) proUserCnt, --省用户量
         |sum(new_user_cnt)  proRegUserCnt, --新注册用户量
         |sum(total_order_cnt) proOrderCnt,--省下单量
         |sum(total_revenue) proRevenue, --省营收
         |sum(total_order_refund_cnt) proRefundOrderCnt,--省退单量
         |sum(total_refund) proRefund
         |from tws_revenue_macstatistic_d
         |group by province
         |
      """.stripMargin).createTempView("temp_tm_revenue_provincerevenue_d")


    session.sql(
      """
        | create EXTERNAL TABLE if not exists TM_revenue_provinceRevenue_d(
        | province string,
        | province_UserCnt   int , --省用户量
        | province_RegUserCnt int, --新注册用户量
        | province_OrderCnt int,--省下单量
        | province_Revenue int, --省营收
        | province_RefundOrderCnt int,--省退单量
        | province_Refund int --省退款
        | )
        | partitioned by (dt_data string)
        | row format delimited
        | fields terminated by '\t'
        | location "hdfs://mycluster/mymusic/hive/tm/revenue/TM_revenue_provinceRevenue_d"
      """.stripMargin)


    session.sql(
      s"""
        | insert overwrite TM_revenue_provinceRevenue_d partition(dt_data=${GenerateDate.dateToString()}) select * from temp_tm_revenue_provincerevenue_d
      """.stripMargin)

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("url","jdbc:mysql://hadoop52:3306/mymusicresult")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

      val currentDate: String = GenerateDate.dateToString()

    session.sql(
      s"""
        |select *  from TM_revenue_provinceRevenue_d
      """.stripMargin).write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop52:3306/mymusicresult","tm_revenue_provincerevenue_d",prop)
  }

}
