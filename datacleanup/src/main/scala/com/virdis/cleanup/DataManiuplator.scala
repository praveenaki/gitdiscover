package com.virdis.cleanup

import com.virdis.cleanup.models._
import org.apache.spark.sql.DataFrame
import Constants._
/**
  * Created by sandeep on 1/19/16.
  */
trait DataManipulator {

  def getAllUserNames(df: DataFrame): DataFrame = df.select( df("actor")("login") )

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame = df.filter( df("type") === eventType )
}
