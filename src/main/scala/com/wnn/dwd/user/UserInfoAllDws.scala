package com.wnn.dwd.user

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserInfoAllDws {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("all User active")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083/mymusic")
      .config("spark.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic")

    import org.apache.spark.sql.functions._
    val alipay: DataFrame = session.table("to_user_alipaybaseinfo_d")
      .withColumn("reg_channel", lit("alipay"))
      .withColumn("ref_channelid", lit("2"))
      .withColumn("ref_uid", col("openid"))
      .select("uid", "reg_mid", "reg_channel", "ref_channelid", "ref_uid", "sex", "birthday", "msisdn", "locationid", "mode_type", "regist_time", "user_exp", "score", "user_level", "user_type", "is_certified", "is_student_certified")

        val qq: DataFrame = session.table("to_user_qqbaseinfo_d")
      .withColumn("reg_channel", lit("QQ"))
      .withColumn("ref_channelid", lit("3"))
      .withColumn("ref_uid", col("openid"))
      //     "user_type", "is_certified", "is_student_certified"
      .withColumn("user_type", lit("2"))
      .withColumn("is_certified", lit("T"))
      .withColumn("is_student_certified", lit("F"))
      .select("uid", "reg_mid", "reg_channel", "ref_channelid", "ref_uid", "sex", "birthday", "msisdn", "locationid", "mode_type", "regist_time", "user_exp", "score", "user_level", "user_type", "is_certified", "is_student_certified")


    val wechat: DataFrame = session.table("to_user_wechatbaseinfo_d")
      .withColumn("reg_channel", lit("wechat"))
      .withColumn("ref_channelid", lit("1"))
      .withColumn("ref_uid", col("wxid"))
      .withColumn("user_type", lit("2"))
      .withColumn("is_certified", lit("T"))
      .withColumn("is_student_certified", lit("F"))
      .select("uid", "reg_mid", "reg_channel", "ref_channelid", "ref_uid", "sex", "birthday", "msisdn", "locationid", "mode_type", "regist_time", "user_exp", "score", "user_level", "user_type", "is_certified", "is_student_certified")


    val app: DataFrame = session.table("to_user_appbaseinfo_d")
      .withColumn("reg_channel", lit("app"))
      .withColumn("ref_channelid", lit("4"))
      .withColumn("ref_uid", col("app_uid"))
      .withColumn("mode_type", lit("0"))
      .withColumn("score", lit("0"))
      .withColumn("user_type", lit("2"))
      .withColumn("is_certified", lit("T"))
      .withColumn("is_student_certified", lit("F"))
      .select("uid", "reg_mid", "reg_channel", "ref_channelid", "ref_uid", "sex", "birthday", "phone_number", "locationid", "mode_type", "regist_time", "user_exp", "score", "user_level", "user_type", "is_certified", "is_student_certified")

    app.unionAll(wechat).unionAll(qq).unionAll(alipay).createTempView("temp_user_info_all")

    session.sql(
      """
        | create external table if not exists tw_user_baseinfo_d(
        | uid                   int          ,
        | reg_mid               int          ,
        | reg_channel   varchar(100)  comment "???????????????0?????????1?????????2????????????3QQ???4APP",
        | ref_channelid	varchar(10)  comment "??????ID???0?????????1?????????2????????????3QQ???4APP",
        | ref_uid			varchar(100)  comment "???????????????ID",
        | sex                   varchar(100) ,
        | birthday              varchar(100) ,
        | phone_number          varchar(100) ,
        | locationid            int          ,
        | reg_mode_type            varchar(10)    comment "???????????????1?????????2?????????0??????"  ,
        | regist_time           varchar(100) ,
        | user_exp              varchar(100) comment "?????????????????????" ,
        | score                 varchar(100)       comment "????????????"   ,
        | user_level            int      comment "????????????"    ,
        | user_type             varchar(10) comment "????????????:1?????????2??????",
        | is_certified          varchar(10) comment "????????????:T??????????????????",
        | is_student_certified  varchar(10) comment "????????????:T??????F??????"
        | )
        | partitioned by(dt_data string)
        | row format delimited
        | fields terminated by '\t'
        | location "hdfs://mycluster/mymusic/hive/dwd/user/tw_user_baseinfo_d"
      """.stripMargin)

    var currentDate: String = GenerateDate.dateToString()
    session.sql(
      s"""
        | INSERT OVERWRITE TABLE tw_user_baseinfo_d partition(dt_data=${currentDate}) select * from temp_user_info_all
      """.stripMargin)



  }

}
