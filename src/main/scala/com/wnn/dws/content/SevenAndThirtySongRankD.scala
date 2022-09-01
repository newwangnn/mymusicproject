package com.wnn.dws.content

import com.wnn.common.GenerateDate
import org.apache.spark.sql.SparkSession

object SevenAndThirtySongRankD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("per7and30Analyse")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions",50)
      .enableHiveSupport()
      .getOrCreate()

    var currentDay=GenerateDate.dateToString()
    println(s"处理歌曲日志日期：${GenerateDate.dateToString()}")
//    var currentDay="20220822"
    var sevenDay=GenerateDate.dateToString(currentDay,7)
    var thirtyDay=GenerateDate.dateToString(currentDay,30)

    session.sql("use mymusic")
    session.sql(
      s"""
        | select
        | songid,
        | count(songid) perSongNBM,
        | 0 perSongSuppNBM,
        | count(distinct uid) perUserSongNBM,
        | count(distinct order_id) perOrderSongNBM
        | from tw_song_play_d
        | where dt_data = ${currentDay}
        | group by songid
        |
      """.stripMargin)
      .createTempView("perSongCNT")

    session.sql(
      s"""
         | select
         | songid,
         | count(songid) perSevenSongNBM,
         | 0 perSevenSongSuppNBM,
         | count(distinct uid) perSevenUserSongNBM,
         | count(distinct order_id) perSevenOrderSongNBM
         | from tw_song_play_d
         | where dt_data between ${sevenDay} and ${currentDay}
         | group by songid
         |
      """.stripMargin)
      .createTempView("sevenSongCNT")

    session.sql(
      s"""
         | select
         | songid,
         | count(songid) perThirtySongNBM,
         | 0 perThirtySongSuppNBM,
         | count(distinct uid) perThirtyUserSongNBM,
         | count(distinct order_id) perThirtyOrderSongNBM
         | from tw_song_play_d
         | where dt_data between ${thirtyDay} and ${currentDay}
         | group by songid
         |
      """.stripMargin)
      .createTempView("thirtySongCNT")

    session.sql(
      s"""
         | select
         | collect_set(o.songid)[0] songid,
         | max(case when o.dt_data between  ${sevenDay} and  ${currentDay} then o.cnt else 0 end ) top7day,
         | max(case when o.dt_data between  ${thirtyDay} and  ${currentDay} then o.cnt else 0 end ) top30day
         | from
         | (select a.dt_data,a.songid, count(a.songid) cnt
         | from tw_song_play_d a
         | where dt_data between  ${thirtyDay} and ${currentDay}
         | group by a.dt_data,a.songid
         | ) o
      """.stripMargin)
      .createTempView("topSongCNT")

    session.sql(
      """
        | select
        | distinct zero.songid,songname,source,album,product,language,video_format,duration,singer1,singerid1,singer2,singerid2,
        | perSongNBM,perSongSuppNBM,perUserSongNBM,perOrderSongNBM,
        | perSevenSongNBM,perSevenSongSuppNBM,perSevenUserSongNBM,perSevenOrderSongNBM,
        | perThirtySongNBM,perThirtySongSuppNBM,perThirtyUserSongNBM,perThirtyOrderSongNBM,
        | top7day,top30day
        | from tw_song_play_d zero
        | left join perSongCNT one
        | on zero.songid=one.songid
        | left join sevenSongCNT seven
        | on zero.songid=seven.songid
        | left join thirtySongCNT thirty
        | on zero.songid=thirty.songid
        | left join topSongCNT top
        | on zero.songid=top.songid
        |
      """.stripMargin)
      .createTempView("tempSevenOrThirtyOrTopD")

    session.sql(
      """
        | create external table if not exists TW_SONG_PLAY_FEATURE_D(
        | songid                     string  ,
        | songname                   string  ,
        | source                     int     ,
        | album                      string  ,
        | product                    string  ,
        | language                   string  ,
        | video_format               string  ,
        | duration                   int     ,
        | singer1                    string  ,
        | singerid1                  string  ,
        | singer2                     string  ,
        | singerid2                  string  ,
        | perSongNBM  int,
        | perSongSuppNBM int,
        | perUserSongNBM int,
        | perOrderSongNBM int,
        | perSevenSongNBM int,
        | perSevenSongSuppNBM int,
        | perSevenUserSongNBM int,
        | perSevenOrderSongNBM int,
        | perThirtySongNBM int,
        | perThirtySongSuppNBM int ,
        | perThirtyUserSongNBM int,
        | perThirtyOrderSongNBM int,
        | top7day int,
        | top30day int
        | )
        | partitioned by (dt_data string)
        | row format delimited
        | fields terminated by '\t'
        | location "hdfs://mycluster/mymusic/hive/dws/song/TW_SONG_PLAY_FEATURE_D"
      """.stripMargin)

    session.sql(
      s"""
        | insert overwrite table TW_SONG_PLAY_FEATURE_D partition(dt_data=${currentDay}) select * from tempSevenOrThirtyOrTopD
      """.stripMargin)
  }

}
