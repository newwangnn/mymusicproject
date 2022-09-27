package com.wnn.dws.user
import org.apache.spark.sql.SparkSession
object DailyUserLoginCNTDws {

   def main(args: Array[String]): Unit = {
      val session: SparkSession = SparkSession.builder()
        .appName("Daily  user login")
//        .master("local")
//        .config("hive.metastore.uris", "thrift://hadoop52:9083/mymusic")
        .config("spark.sql.shuffle.partitions", "10")
        .enableHiveSupport()
        .getOrCreate()
      session.sql("use mymusic")

      session.sql(
        """
          |create external table if not exists TW_User_DailyLoginCNT_D(
          |logintime date,
          |dailyLoginUserCNT int
          |)
          |row format delimited
          |fields terminated by '\t'
          |location "hdfs://mycluster/mymusic/hive/dws/user/TW_User_DailyLoginCNT_D"
        """.stripMargin)

      session.sql(
        """
          |	SELECT loginTm ,count(uid) dailyLoginUserCNT from (
          |		select a.uid,a.mid,TO_DATE(CONCAT_ws ("-",SUBSTR(a.logintime,0,4),SUBSTR(a.logintime,5,2),SUBSTR(a.logintime,7,2))) loginTm ,
          |		TO_DATE(CONCAT_ws("-", SUBSTR(b.regist_time,0,4),SUBSTR(b.regist_time,5,2),SUBSTR(b.regist_time,7,2))) registTm
          |		from to_user_logininfo_d a
          |		inner join tw_user_baseinfo_d b
          |		on a.uid=b.uid
          |	) c
          |	where loginTm  is not null
          |	group by loginTm
          |	order by loginTm
        """.stripMargin).createTempView("temp_userlogincnt")

      session.sql(
        """
          | insert overwrite table TW_User_DailyLoginCNT_D select * from temp_userlogincnt
        """.stripMargin)
    }

  }


