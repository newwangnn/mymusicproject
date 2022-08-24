package com.wnn.common

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class KVMultipleTextOutputFormat extends MultipleTextOutputFormat[String,String]{


  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = {
    key
  }

  override def generateActualKey(key: String, value: String): String = {
    null
  }
}
