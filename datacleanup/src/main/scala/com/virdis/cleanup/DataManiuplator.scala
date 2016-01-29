package com.virdis.cleanup

import org.apache.spark.sql.{SQLContext, DataFrame}
import Constants._

/**
  * Created by sandeep on 1/19/16.
  */
trait DataManipulator {

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame = df.filter( df(EVENT_TYPE) === eventType )

  /*
      lazy load S3 File Handles
   */

  def s3FileHandle(idx: Int)(implicit sqlContext: SQLContext) = {
    sqlContext.read.json(S3_FILENAMES(idx))
  }
}
