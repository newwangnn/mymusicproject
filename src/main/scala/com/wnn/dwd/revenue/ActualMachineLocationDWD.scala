package com.wnn.dwd.revenue

import com.alibaba.fastjson2.JSONObject
import com.wnn.common.GenerateDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalaj.http.{Http, HttpResponse}

import scala.beans.BeanProperty

case class AddressComponent (mid:Int,addressDetail:String,province:String,city:String,township:String,street:String,number:String,xy:String,direction:String,distance:String)
//class AddressComponent extends Serializable{
//  @BeanProperty
//  var mid:Int=0
//  @BeanProperty
//  var addressDetail:String=""
//  @BeanProperty
//  var province:String=""
//  @BeanProperty
//  var city:String="";
//  @BeanProperty
//  var township:String=""
//  @BeanProperty
//  var street:String=""
//  @BeanProperty
//  var number:String=""
//  @BeanProperty
//  var xy:String=""
//  @BeanProperty
//  var direction:String=""
//  @BeanProperty
//  var distance:String=""
//}
object ActualMachineLocationDWD {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("通过用户位置日志获取每台机器的真实位置信息")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .config("spark.sql.shuffle.partitions", 5)
      .enableHiveSupport()
      .getOrCreate()
    session.sql("use mymusic ")

  session.sql(
      """
        |  select mid,x,y
        |  from to_revenue_userlocation_d
        |  where x <> "" and y <> ""
      """.stripMargin)
    .createTempView("temp_userlocation")

    val resultRdd: RDD[AddressComponent] = session.table("temp_userlocation")
      .rdd
      .mapPartitions(
        (piter) => {
          piter.map(r => {
            val mid: Int = r.getAs[Int]("mid")
            val x: String = r.getAs[String]("x")
            val y: String = r.getAs[String]("y")

            val str: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo?parameters")
              .param("key", "ea22791d1566a92ad36bfec796a11a1e")
              .param("location", y + "," + x)
              .asString

            val nObject: JSONObject = JSONObject.parseObject(str.body)
            try{
            val targetAddr: JSONObject = nObject.getJSONObject("regeocode")
            val formatted_address: String = targetAddr.getString("formatted_address")

            val targetdetail: JSONObject = targetAddr.getJSONObject("addressComponent")
            val province: String = targetdetail.getString("province")
            val city: String = targetdetail.getString("city")
            val township: String = targetdetail.getString("township")
            val streetObject: JSONObject = targetdetail.getJSONObject("streetNumber")
            val street: String = streetObject.getString("street")
            val number: String = streetObject.getString("number")
            val xy: String = streetObject.getString("location")
            val direction: String = streetObject.getString("direction")
            val distance: String = streetObject.getString("distance")
            println(s"${xy}--/////---${x}--${y}")
//            val component = new AddressComponent()
//            component.setMid(mid)
//            component.setAddressDetail(formatted_address)
//            component.setProvince(province)
//            component.setCity(city)
//            component.setTownship(township)
//            component.setStreet(street)
//            component.setNumber(number)
//            component.setXy(xy)
//            component.setDirection(direction)
//            component.setDistance(distance)
//            component
            AddressComponent(mid, formatted_address, province, city, township, street, number, xy, direction, distance)
            }catch {
              case e:Exception=>{
                  AddressComponent(0,"","","","","","","","","")
              }
            }
          })
        }
      )

//    resultRdd.foreach(println)
//    case class AddressComponent (mid:Int,addressDetail:String,province:String,city:String,township:String,street:String,number:String,xy:String,direction:String,distance:String)
//    val df: DataFrame = session.createDataFrame(resultRdd,classOf[AddressComponent])
//////////////////////////////隐式导入scala中的样例类进行了序列化操作
    import session.implicits._
    resultRdd.toDF().createTempView("temp_mid_location")
    session.sql(
      """
        | create external table if not exists TWD_revenue_machineactuallocation_d(
        | mid   Int,
        |addressDetail   String,
        |province   String,
        |city   String,
        |township   String,
        |street   String,
        |number   String,
        |xy   String,
        |direction   String,
        |distance   String
        |)
        | partitioned by(dt_data string)
        | row format delimited
        | fields terminated by '\t'
        | location "hdfs://mycluster/mymusic/hive/dwd/revenue/TWD_revenue_machineactuallocation_d"
        |
      """.stripMargin)

    session.sql(
      s"""
        | insert overwrite table TWD_revenue_machineactuallocation_d partition(dt_data=${GenerateDate.dateToString()}) select * from temp_mid_location
      """.stripMargin)

  }

}
