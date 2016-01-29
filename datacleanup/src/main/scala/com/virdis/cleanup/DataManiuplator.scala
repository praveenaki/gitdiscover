package com.virdis.cleanup

import org.apache.spark.sql.{SQLContext, DataFrame}
import Constants._

/**
  * Created by sandeep on 1/19/16.
  */
trait DataManipulator {

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame = df.filter( df(EVENT_TYPE) === eventType )
}
