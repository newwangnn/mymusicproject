package com.wnn.dm.mac

import java.util.Properties

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

object ProvinceMacCntD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("每日机器在各省份的分布情况")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("saprk.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()

    session.sql("use mymusic")

    session.sql(
      """
        | create external table if not exists TM_mac_provinceCNT_d(
        |  province string,
        |  total int
        | )
        | partitioned by(dt_data string)
        | row format delimited
        | fields terminated by '\t'
        | location "hdfs://mycluster/mymusic/hive/tm/mac/TM_mac_provinceCNT_d"
      """.stripMargin)

    session.sql(
      """
        | SELECT
        | province ,COUNT(mid)
        | from tw_mac_baseinfo_d
        | group by province
      """.stripMargin).createTempView("temp_province_totalCNT")

    val currentDate: String = GenerateDate.dateToString()

    session.sql(
      s"""
        | insert into TM_mac_provinceCNT_d partition(dt_data=${currentDate}) select * from temp_province_totalCNT
      """.stripMargin)

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("url","jdbc:mysql://hadoop52:3306/mymusicresult")
    prop.setProperty("driver","com.mysql.jdbc.Driver")


    session.table("temp_province_totalCNT")
      .withColumn("dt_data",lit(currentDate))
      .write
      .mode(SaveMode.Overwrite)
      .option("useUnicode","true")
      .option("characterEncoding","utf-8")
      .jdbc("jdbc:mysql://hadoop52:3306/mymusicresult","tm_province_cnt_d",prop)

  }

}
