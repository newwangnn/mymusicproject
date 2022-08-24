package com.wnn.ods


import com.alibaba.fastjson2.JSONObject
import com.wnn.common.{GenerateDate, KVMultipleTextOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SparkSession}


object ClientPlaySong {



  def main(args: Array[String]): Unit = {


    val session: SparkSession = SparkSession.builder()
      .appName("cluster runtime")
      .master("local")
      .config("hive.metastore.uris","thrift://hadoop52:9083")
      .config("spark.sql.warehouse.dir","/mymusic/hive")
      .enableHiveSupport()
      .getOrCreate()
    val src: RDD[String] = session.sparkContext.textFile("hdfs://mycluster/musictestdata/currentday_clientlog.tar.gz")

    src.filter(line=>{
      line.split("&").length==6
    })
      .map(line=>{
        val strs: Array[String] = line.split("&")
        if("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(strs(2))){
          val jsonObj: JSONObject = JSONObject.parseObject(strs(3))
          val songid: String = jsonObj.getString("songid")
          val mid: String = jsonObj.getString("mid")
          val optrate_type: String = jsonObj.getString("optrate_type")
          val uid: String = jsonObj.getString("uid")
          val consume_type: String = jsonObj.getString("consume_type")
          val play_time: String =jsonObj.getString("play_time")
          val dur_time: String = jsonObj.getString("dur_time")
          val session_id: String = jsonObj.getString("session_id")
          val songname: String = jsonObj.getString("songname")
          val pkg_id: String = jsonObj.getString("pkg_id")
          val order_id: String = jsonObj.getString("order_id")

          ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ",songid+"\t"+mid+"\t"+optrate_type+"\t"+uid+"\t"+consume_type+"\t"+play_time+"\t"+dur_time+"\t"+session_id+"\t"+songname+"\t"+pkg_id+"\t"+order_id)
        }else{
          (strs(2),strs(3))
        }
    })
//      local本地執行代碼
//      .foreach(println)
//      .saveAsHadoopFile("d:\\mycluster/mymusic/datasource/",classOf[String],classOf[String],classOf[KVMultipleTextOutputFormat])
      .saveAsHadoopFile("hdfs://mycluster/mymusic/datasource/",classOf[String],classOf[String],classOf[KVMultipleTextOutputFormat])


    session.sql(
      """
        |  create database if not exists mymusic
      """.stripMargin)
    session.sql(
      """
        |  use mymusic
      """.stripMargin)
    session.sql(
      """
        | create EXTERNAL table if not exists TO_CLIENT_SONG_PLAY_OPERATE_REQ_D(
        | songid string,
        | mid string,
        | optrate_type string,
        | uid string,
        | consume_type string,
        | play_time string,
        | dur_time string,
        | session_id  string,
        | songname   string,
        | pkg_id  string,
        | order_id string
        | )
        | PARTITIONED BY (dt_data string)
        | ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        | location "hdfs://mycluster/mymusic/hive/ods/song/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D"
        |
      """.stripMargin)

    session.sql(
      s"""
        |  load data inpath "hdfs://mycluster/mymusic/datasource/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ" into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D PARTITION (dt_data=${GenerateDate.dateToString()})
      """.stripMargin)

  }

}
