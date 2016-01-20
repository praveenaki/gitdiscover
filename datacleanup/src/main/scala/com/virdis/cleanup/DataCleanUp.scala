package com.virdis.cleanup

import com.virdis.cleanup.models.{CommitCommentEventData, GitData}
import org.apache.spark.sql.DataFrame
/**
  * Created by sandeep on 1/19/16.
  */
trait DataCleanUp {

  def getAllUserNames(df: DataFrame): DataFrame = df.select( df("actor")("login") )

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame =
    df.filter( df("type") === eventType )

  def getAllCommentByEventType(df: DataFrame, eventType: String): DataFrame = {
    eventType match {
      case "CommitCommentEvent" => df.select( df("payload")("comment") )
      case "IssueCommentEvent" => df.select(  df("payload")("comment"))
    }
  }



  def splitDataByEventType(eventType: String, df: DataFrame): GitData = {
    eventType.toLowerCase match {
      case "commitcommentevent" => {
        CommitCommentEventData(eventType,
          df.select(df("payload")("comment")),
          df.select("created_at"),
          df.select(df("actor")("login")),
          df.select(df("repo")("name"))
        )
      }
    }
  }
}
