package com.wnn.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object GenerateDate {



  def dateToString():String={
    val l: Long = System.currentTimeMillis()
    val format = new SimpleDateFormat("yyyyMMdd")
    val date = new Date(l)
    val str: String = format.format(date)
    str
  }
  def dateToString(date:String,nDay:Int):String={
    var str="19700101"
    try{
      val format = new SimpleDateFormat("yyyyMMdd")
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(format.parse(date))
      calendar.add(Calendar.DAY_OF_MONTH,-nDay)
      str=format.format(calendar.getTime)

    }catch {
      case e:Exception=>{

      }
    }
    str
  }

  def dateToString(time:String):String={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    var str=""
    try{
      val date = new Date(time)
      str = format.format(date)
    }catch {
      case e:Exception =>{

      }
    }

    str
  }

  def main(args: Array[String]): Unit = {
    var d="20220819"
    val str: String = dateToString(d,30)
    print(str)
  }

}
