package com.wnn.dm.song

import java.util.Properties

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SongRsiD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("per song rank ")
      .master("local")
      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .enableHiveSupport()
      .getOrCreate()

//    var currentDate=GenerateDate.dateToString()
    var currentDate=20220822


    session.sql("use mymusic")

    import org.apache.spark.sql.functions._

    session.sql(
      s"""
        | select * from tw_song_play_feature_d where dt_data=${currentDate}
      """.stripMargin)
      .withColumn("RSI_1",pow(log(col("persongnbm")/1+1)*0.63*0.8+log(col("persongsuppnbm")/1+1)*0.63*0.2,2)*10)
      .withColumn("RSI_7",pow(
        (log(col("persevensongnbm")/7+1)*0.63*0.8+log(col("persevensongsuppnbm")/7+1)*0.2)
      +
        log(col("top7day")/7+1)*0.37*0.8
      ,2))
      .withColumn("RSI_30",pow(
        (log(col("perthirtysongnbm")/7+1)*0.63*0.8+log(col("perthirtysongsuppnbm")/7+1)*0.2)
          +
          log(col("top30day")/7+1)*0.37*0.8
     ,2 )).createTempView("temp_song_rsi_d")

    val d1: DataFrame = session.sql(
      """
 select "1"as period,songid,songname,singer1,singe2,RSI_1 as RSI,
 row_number() over(partition by dt_data order by RSI_1) as RSI_RANK
 from temp_song_rsi_d
      """.stripMargin)

    val d7: DataFrame = session.sql(
      """
 select "7"as period,songid,songname,singer1,singe2,RSI_7 as RSI,
 row_number() over(partition by dt_data order by RSI_7) as RSI_RANK
 from temp_song_rsi_d
      """.stripMargin)

    val d30: DataFrame = session.sql(
      """
 select "30"as period,songid,songname,singer1,singe2,RSI_30 as RSI,
 row_number() over(partition by dt_data order by RSI_30) as RSI_RANK
 from temp_song_rsi_d
      """.stripMargin)

    d1.union(d7).union(d30).createTempView("temp_tm_song_rsi")

    session.sql(
      """
        | create external table if not exists TM_SONG_RSI_D (
        | period string,
        | songid string,
        | songname string,
        | singer1 string,
        | singer2 string,
        | RSI double,
        | RSI_RANK int
        | )
        | partitioned by (dt_data string)
        | row format delimited
        | fields terminated by '\t'
        | location "hdfs://mycluster/mymusic/hive/tm/song/TM_SONG_RSI_D"
      """.stripMargin)

    session.sql(
      s"""
        | insert overwrite table TM_SONG_RSI_D partition(dt_data=${currentDate}) select period,songid,songname,singer1,singe2,RSI,RSI_RANK from temp_tm_song_rsi
      """.stripMargin)

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("url","jdbc:mysql://hadoop52:3306/mymusic")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    session.sql(
      """
        | select * from temp_tm_song_rsi
      """.stripMargin)
      .write
      .mode(SaveMode.Append)
      .option("useUnicode","true")
      .option("characterEncoding","utf-8")
      .jdbc("jdbc:mysql://hadoop52:3306/mymusic","tm_song_rsi_d",prop)

  }

}
