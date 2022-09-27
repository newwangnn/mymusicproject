package com.wnn.dws.mac

import com.wnn.common.GenerateDate
import org.apache.spark.sql.{DataFrame, SparkSession}

object TwMACinfoD {

  def provinceHandle :String=>String = (provinceStr:String)=>{
    var province = ""
    try{
      province= provinceOrCity(provinceStr,"省")
    }catch {
      case e:Exception=>{

      }
    }
    province
  }

  def cityHandle:String=>String=(citystr:String)=>{
    var city=""
    try{
      city= provinceOrCity(citystr,"城")
    }catch {
      case e:Exception=>{

      }
    }
    city
  }

//  flag:省与城
  def provinceOrCity(proString:String,flag:String)={
    var result=""
    var provinceIndex = -1
    if(!proString.isEmpty){
      if(proString.contains("自治区")){
        provinceIndex= proString.indexOf("自治区")
      }else if(proString.contains("省")){
        provinceIndex= proString.indexOf("省")
      }else if(proString.contains("市")){
        provinceIndex= proString.indexOf("市")
      }

      if("省".equals(flag)){
        result=proString.substring(0,provinceIndex)
      }
      if("城".equals(flag)){
        if(proString.contains("省") && proString.contains("市") ){
          result=proString.substring(provinceIndex+1,proString.indexOf("市"))
        }else if( proString.contains("自治区")&& proString.contains("市")){
          result=proString.substring(provinceIndex+3,proString.indexOf("市"))
        }else if(proString.contains("市")){
          result=proString.substring(0,provinceIndex)
        }
      }
    }
    result
  }

  def timeHandle:String=>String=(timestr:String)=>{
    var time=timestr
    try{
      if("null".equals(timestr)){
        time="20150814000000"
      }
    }catch {
      case e:Exception=>{}
    }
    time
  }

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("processMacInfo")
      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("saprk.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.functions._

  val province = udf(provinceHandle)
  val city = udf(cityHandle)
  val getTime = udf(timeHandle)

    session.sql(" use mymusic ")

    val macLocal1: DataFrame = session.table("to_mac_machinelocalinfo_d")
      .where(" address  != 'null' and province = 'null' and city ='null' ")
      .withColumn("province", province(col("address")))
      .withColumn("city", city(col("address")))
      .withColumn("revenue_time", getTime(col("revenue_time")))
      .withColumn("sale_time", getTime(col("sale_time")))
      .select("mid", "province", "city", "map_class", "mglng", "mglat", "address", "real_address", "revenue_time", "sale_time")

    session.table("to_mac_machinelocalinfo_d")
      .where(" address  != 'null' and province != 'null' and city !='null' ")
      .select("mid","province","city","map_class","mglng","mglat","address","real_address","revenue_time","sale_time")
      .unionAll(macLocal1)
      .createTempView("temp_maclocal")

    ///////////////
    session.table("to_mac_machinestoreinfo_d")
      .select("id","sub_tag_name","store_address","ground_name","store_opening_time","store_closing_time","sub_scence_cate_name","sub_scence_name","brand_name","sub_brand_name")
      .createTempView("temp_store")

    session.table("to_mac_machineadminmap_d")
      .select("machine_num","machine_name","package_num","package_name","inv_rate","age_rate","com_rate","par_rate","deposit","scene_address","product_type","serial_num","had_mpay_func","is_activate","activate_time","order_time","ground_name")
      .createTempView("temp_admin")

    session.sql(
      """
        |select
        |mli.*,
        |msi.*,
        |mb.serialnum,mb.hard_id,mb.song_warehouse_version,mb.exec_version,mb.ui_version,mb.online,mb.status,mb.current_login_time,mb.pay_switch,mb.language_type,mb.songware_type,mb.screen_type,
        |mam.machine_num,mam.machine_name,mam.package_num,mam.package_name,mam.inv_rate,mam.age_rate,mam.com_rate,mam.par_rate,mam.deposit,mam.scene_address,mam.product_type,mam.serial_num,mam.had_mpay_func,mam.is_activate,mam.activate_time,mam.order_time,
        |msm.adminid,msm.create_time
        |from to_mac_machinestoremap_d msm
        |join temp_store msi
        |on msm.store_id =msi.id
        |join temp_maclocal mli
        |on msm.machine_num = mli.mid
        |JOIN to_mac_machinebaseinfo_d mb
        |on msm.machine_num =mb.mid
        |join temp_admin mam
        |on msm.machine_num = mam.machine_num
        |
      """.stripMargin).createTempView("temp_tw_mac_baseinfo_d")

    session.sql(
      """
        | create external table if not exists TW_MAC_BASEINFO_D(
        |mid            int                ,
        |province       varchar(100)            ,
        |city           varchar(100)            ,
        |map_class      varchar(100)            ,
        |mglng          varchar(100)            ,
        |mglat          varchar(100)            ,
        |address        varchar(100)            ,
        |real_address   varchar(100)            ,
        |revenue_time   varchar(100)            ,
        |sale_time      varchar(100)            ,
        |id                     int        ,
        |sub_tag_name           varchar(100)    ,
        |store_address          varchar(100)    ,
        |ground_name            varchar(100)    ,
        |store_opening_time     varchar(100)    ,
        |store_closing_time     varchar(100)    ,
        |sub_scence_cate_name   varchar(100)    ,
        |sub_scence_name        varchar(100)    ,
        |brand_name             varchar(100)    ,
        |sub_brand_name         varchar(100)    ,
        |serialnum                varchar(100)  ,
        |hard_id                  varchar(100)  ,
        |song_warehouse_version   varchar(100)  ,
        |exec_version             varchar(100)  ,
        |ui_version               varchar(100)  ,
        |online                   varchar(1)    ,
        |status                   int       ,
        |current_login_time       varchar(100)  ,
        |pay_switch               varchar(100)  ,
        |language_type            int       ,
        |songware_type            int       ,
        |screen_type              int       ,
        |machine_num       int             ,
        |machine_name      varchar(100)         ,
        |package_num       int            ,
        |package_name      varchar(100)         ,
        |inv_rate          double        ,
        |age_rate          double        ,
        |com_rate          double         ,
        |par_rate          double         ,
        |deposit           double         ,
        |scene_address     varchar(100)         ,
        |product_type      int           ,
        |serial_num        varchar(100)         ,
        |had_mpay_func     int             ,
        |is_activate       int             ,
        |activate_time     varchar(100)         ,
        |order_time        varchar(100)         ,
        |adminid       int                 ,
        |create_time   varchar(100)
        |)
        |partitioned by (dt_data string)
        |ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        |LOCATION 'hdfs://mycluster/mymusic/hive/dws/mac/TW_MAC_BASEINFO_D'
      """.stripMargin)

    val currentdate: String = GenerateDate.dateToString()
    session.sql(
      s"""
        | INSERT OVERWRITE TABLE TW_MAC_BASEINFO_D PARTITION (dt_data=${currentdate}) select * FROM temp_tw_mac_baseinfo_d
      """.stripMargin)

  }

}
