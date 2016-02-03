package com.virdis.cleanup

import org.apache.spark.sql.{SQLContext, DataFrame}
import Constants._

/**
  * Created by sandeep on 1/19/16.
  */
trait CommonDataFunctions {

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame = df.filter( df(EVENT_TYPE) === eventType )

  /*
      lazy load S3 File Handles
   */

  def s3FileHandle(idx: Int)(implicit sqlContext: SQLContext) = {
    sqlContext.read.json(S3_FILENAMES(idx))
  }

  def repoAndLanguageDF(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val pullReqs = getDataByEventType(df, PULL_REQUEST_EVENT)

    pullReqs.filter(pullReqs(PULL_REQ_LANGUAGE_COLUMN).isNotNull).select(pullReqs(REPO_NAME_COLUMN),
      pullReqs(PULL_REQ_LANGUAGE_COLUMN))
  }

  def getData(sQLContext: SQLContext) = {
    val df = sQLContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "popularrepos", "keyspace" -> "git" ))
      .load()

    df.select( df(NAME_COLUMN), df(LANGUAGE_COLUMN) )

  }

}
