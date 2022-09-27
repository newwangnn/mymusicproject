package com.wnn.dm.user

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object WeekUserLoginCntDM {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("week user login CNT")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083/mymusic")
      .config("spark.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic")

    session.sql(
      """
        |create external table if not exists TM_User_weekLoginCNT_D(
        |logintime date,
        |weekLoginUserCNT bigint
        |)
        |row format delimited
        |fields terminated by '\t'
        |location "hdfs://mycluster/mymusic/hive/tm/user/TM_User_weekLoginCNT_D"
      """.stripMargin)

    session.sql(
      """
        |  SELECT  logintime,sum(dailyLoginUserCNT) over(order by logintime  rows BETWEEN 7 PRECEDING and current row) weekLoginUserCNT
        | from tw_user_dailylogincnt_d
      """.stripMargin).createTempView("temp_userlogincnt")

    session.sql(
      """
        | insert overwrite table TM_User_weekLoginCNT_D select * from temp_userlogincnt
      """.stripMargin)

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("url","jdbc:mysql://hadoop52:3306/mymusicresult")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    session.sql(
      """
        | select * from temp_userlogincnt
      """.stripMargin).write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop52:3306/mymusicresult","tm_weekLoginUser_cnt_d",prop)
  }

}
