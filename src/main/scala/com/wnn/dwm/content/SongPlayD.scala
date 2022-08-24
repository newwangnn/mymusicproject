package com.wnn.dwm.content

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{SaveMode, SparkSession}

object SongPlayD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName(" per Song play ")
      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.functions._
    var date=GenerateDate.dateToString()
//    var date="20220822"

    session.sql("use mymusic")

    session.sql(
      """
        | create external table if not exists TW_SONG_PLAY_D(
        |songid                     string  ,
        |mid                        string  ,
        |optrate_type               string  ,
        |uid                        string  ,
        |consume_type               string  ,
        |play_time                  string  ,
        |dur_time                   string  ,
        |session_id                 string  ,
        |songname                   string  ,
        |pkg_id                     string  ,
        |order_id                   string  ,
        |
        |source                     int     ,
        |album                      string  ,
        |product                    string  ,
        |language                   string  ,
        |video_format               string  ,
        |duration                   int     ,
        |singer1                    string  ,
        |singerid1                  string  ,
        |singer2                     string  ,
        |singerid2                  string  ,
        |post_time                  string  ,
        |pinyin_first               string  ,
        |pinyin                     string  ,
        |singing_type               int     ,
        |original_singer            string  ,
        |lyricist                   string  ,
        |composer                   string  ,
        |bpm                        int     ,
        |star_level                 int     ,
        |video_quality              int     ,
        |video_make                 int     ,
        |video_feature              int     ,
        |lyric_feature              int     ,
        |image_quality              int     ,
        |subtitles_type             int     ,
        |audio_format               int     ,
        |original_sound_quality     int     ,
        |original_track             int     ,
        |original_track_vol         int     ,
        |accompany_version          int     ,
        |accompany_quality          int     ,
        |acc_track_vol              int     ,
        |accompany_track            int     ,
        |width                      int     ,
        |height                     int     ,
        |video_resolution           int     ,
        |song_version               int     ,
        |authorized_company         string  ,
        |status                     int     ,
        |publish_to                 array<int>
        |)
        |partitioned by (dt_data string)
        |row format delimited
        |fields terminated by '\t'
        | collection items terminated by ','
        | location "hdfs://mycluster/mymusic/hive/dwm/song/TW_SONG_PLAY_D"
        |
      """.stripMargin)

    session.sql(
      s"""
        | select *
        |  from tw_song_info_d
        |  inner join to_client_song_play_operate_req_d
        |  on songid=source_id
        |  where dt_data = ${date}
      """.stripMargin)
//      .show(50)
//      .join(session.table("tw_song_info_d"),col("songid") === col("source_id"),"inner").show()
      .createTempView("tempSongPlayD")

    session.sql(
      s"""
        | insert overwrite table TW_SONG_PLAY_D partition(dt_data=${date})
        |  select
        |     songid,mid,optrate_type,uid,consume_type,play_time,dur_time,session_id,songname,pkg_id,order_id,
        |  source,album,product,language,
        |  video_format,duration,singer1,singerid1,singe2,singerid2,post_time,pinyin_first,
        |  pinyin,singing_type,original_singer,lyricist,composer,bpm,star_level,
        |  video_quality,video_make,video_feature,lyric_feature,image_quality,subtitles_type,
        |  audio_format,original_sound_quality,original_track,original_track_vol,accompany_version,
        |  accompany_quality,acc_track_vol,accompany_track,width,height,video_resolution,
        |  song_version,authorized_company,status,publish_to
        |   from tempSongPlayD
      """.stripMargin)

  }



}
