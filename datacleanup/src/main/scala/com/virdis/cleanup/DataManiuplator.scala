package com.virdis.cleanup

import com.virdis.cleanup.models._
import org.apache.spark.sql.{SQLContext, DataFrame}
import Constants._
/**
  * Created by sandeep on 1/19/16.
  */
trait DataManipulator {

  def getAllUserNames(df: DataFrame): DataFrame = df.select( df("actor")("login") )

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame = df.filter( df("type") === eventType )

  val listOfFileNames = List(
    BUCKET_PATH + "JanFull.json",
    BUCKET_PATH + "FebFull.json",
    BUCKET_PATH + "MarFull.json",
    BUCKET_PATH + "AprilFull.json",
    BUCKET_PATH + "MayFull.json",
    BUCKET_PATH + "JunFull.json",
    BUCKET_PATH + "JulFull.json",
    BUCKET_PATH + "AugFull.json",
    BUCKET_PATH + "SepFull.json",
    BUCKET_PATH + "OctFull.json",
    BUCKET_PATH + "NovFull.json",
    BUCKET_PATH + "DecFull.json"
  )

  def uberDataFrame(sQLContext: SQLContext) = {
    var df: DataFrame = sQLContext.read.json(listOfFileNames.head)
    listOfFileNames.tail.foreach(s => df = df.unionAll(sQLContext.read.json(s)))

    df
  }
}
