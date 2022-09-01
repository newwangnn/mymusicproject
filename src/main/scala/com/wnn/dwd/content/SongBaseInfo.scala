package com.wnn.dwd.content

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object SongBaseInfo {

  def handleAlbum: String => String = (album: String) => {
    var result = ""
    try{
      if (album.contains("《")) {
        result = album.substring(album.indexOf("《"), album.lastIndexOf("》") + 1)
      }
    }catch {
      case e:Exception=>{

      }
    }

    result
  }

  def handleSingerInfo: (String, String, String) => String = (singerInfo: String, mark: String, name: String) => {
    var result = ""
    try {
      val array: JSONArray = JSON.parseArray(singerInfo)

      if ("mark1".equals(mark) && array.size() > 0) {
        if ("singer1".equals(name)) {
          result = array.getJSONObject(0).getString("name")
        } else if ("id1".equals(name)) {
          result = array.getJSONObject(0).getString("id")
        }
      } else if ("mark2".equals(mark) && array.size() > 1) {
        if ("singer2".equals(name)) {
          result = array.getJSONObject(0).getString("name")
        } else if ("id2".equals(name)) {
          result = array.getJSONObject(0).getString("id")
        }
      }
      //      if("mark1".equals(mark) && "singer1".equals(name) && array.size()>0){
      //        result= array.getJSONObject(0).getString("name")
      //      }else if("mark1".equals(mark) && "id1".equals(name) && array.size()>0){
      //        result = array.getJSONObject(0).getString("id")
      //      }else if ("mark2".equals(mark) && "singer2".equals(name) && array.size()>0){
      //        result= array.getJSONObject(0).getString("name")
      //      }else if("mark2".equals(mark) && "id2".equals(name) && array.size()>0){
      //        result = array.getJSONObject(0).getString("id")
      //      }

    } catch {
      case e: Exception => {
        result
      }
    }
    result
  }

  def handlePostTime: String => String = (postTime: String) => {
    var result = ""
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val date = new Date(postTime)
      result = format.format(date)

    } catch {
      case e: Exception => {
        result = "1970-01-01"
      }
    }
    result
  }

  def handleAuthorizedCompany: String => String = (authorizedCompany: String) => {
    //    {"name":"乐心曲库","id":"5aa740ecb19efe0b504ce30a"}
    var result = "自定义曲库"
    try {

      result = JSON.parseObject(authorizedCompany).getString("name")
    } catch {
      case e: Exception => {

      }
    }
    result
  }

  def handlePublishTo: String => ListBuffer[Int] = (publishTo: String) => {
    //    [2.0,8.0]
    val ints = new ListBuffer[Int]
    try {
      if (publishTo.contains("[")) {
        val arrs: Array[String] = publishTo.trim.stripPrefix("[").stripSuffix("]").split(",")
        arrs.foreach(f => {
          ints.append(f.toDouble.toInt)
        })
      }

    } catch {
      case e: Exception => {

      }
    }
    ints
  }

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName(" extract data ")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions",50)
      .enableHiveSupport()
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    import org.apache.spark.sql.functions._

    val myAlbum = udf(handleAlbum)
    val mySingerInfo = udf(handleSingerInfo)
    val myPostTime = udf(handlePostTime)
    val myAuthorizedCompany = udf(handleAuthorizedCompany)
    val myPublishTo = udf(handlePublishTo)


    session.sql(" use mymusic ")
    session.table("to_song_info_d")
      .select(
        col("source_id"),
        col("name"),
        col("source"),
        myAlbum(col("album")),
        col("product"),
        col("language"),
        col("video_format"),
        col("duration"),
        mySingerInfo(col("singer_info"), lit("mark1"), lit("singer1")),
        mySingerInfo(col("singer_info"), lit("mark1"), lit("id1")),
        mySingerInfo(col("singer_info"), lit("mark2"), lit("singer2")),
        mySingerInfo(col("singer_info"), lit("mark2"), lit("id2")),
        myPostTime(col("post_time")),
        col("pinyin_first"),
        col("pinyin"),
        col("singing_type"),
        col("original_singer"),
        col("lyricist"),
        col("composer"),
        col("bpm"),
        col("star_level"),
        col("video_quality"),
        col("video_make"),
        col("video_feature"),
        col("lyric_feature"),
        col("image_quality"),
        col("subtitles_type"),
        col("audio_format"),
        col("original_sound_quality"),
        col("original_track"),
        col("original_track_vol"),
        col("accompany_version"),
        col("accompany_quality"),
        col("acc_track_vol"),
        col("accompany_track"),
        col("width"),
        col("height"),
        col("video_resolution"),
        col("song_version"),
        myAuthorizedCompany(col("authorized_company")),
        col("status"),
        myPublishTo(col("publish_to"))
      ).createTempView("temp_song_info_d")

    session.sql(
      """
        | create external table if not exists TW_SONG_INFO_D(
        |source_id               string ,
        |name                    string ,
        |source                  int    ,
        |album                   string ,
        |product                 string ,
        |language                string ,
        |video_format            string ,
        |duration                int    ,
        |singer1             string ,
        |singerid1             string ,
        |singer2             string ,
        |singerid2             string ,
        |post_time               string ,
        |pinyin_first            string ,
        |pinyin                  string ,
        |singing_type            int    ,
        |original_singer         string ,
        |lyricist                string ,
        |composer                string ,
        |bpm                     int    ,
        |star_level              int    ,
        |video_quality           int    ,
        |video_make              int    ,
        |video_feature           int    ,
        |lyric_feature           int    ,
        |image_quality           int    ,
        |subtitles_type          int    ,
        |audio_format            int    ,
        |original_sound_quality  int    ,
        |original_track          int    ,
        |original_track_vol      int    ,
        |accompany_version       int    ,
        |accompany_quality       int    ,
        |acc_track_vol           int    ,
        |accompany_track         int    ,
        |width                   int    ,
        |height                  int    ,
        |video_resolution        int    ,
        |song_version            int    ,
        |authorized_company      string ,
        |status                  int    ,
        |publish_to              Array<Int>
        | )
        | row format delimited
        | fields terminated by '\t'
        | collection items terminated by ','
        | location "hdfs://mycluster/mymusic/hive/dwd/song/TW_SONG_INFO_D"
      """.stripMargin)

    session.sql(
      """
        | INSERT OVERWRITE TABLE TW_SONG_INFO_D select * from  temp_song_info_d
      """.stripMargin)



  }

}
