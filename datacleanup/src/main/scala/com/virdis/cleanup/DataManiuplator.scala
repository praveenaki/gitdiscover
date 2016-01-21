package com.virdis.cleanup

import com.virdis.cleanup.models._
import org.apache.spark.sql.DataFrame
import Constants._
/**
  * Created by sandeep on 1/19/16.
  */
trait DataManipulator {

  def getAllUserNames(df: DataFrame): DataFrame = df.select( df("actor")("login") )

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame =
    df.filter( df("type") === eventType )


  /*def splitDataByEventType(eventType: String, df: DataFrame): GitData = {
    val filterByEventDF = getDataByEventType(df, eventType)
    eventType.toLowerCase match {
      case (COMMIT_COMMENT_EVENT.toLowerCase) => {
        CommitCommentEventData(eventType,
          filterByEventDF.select(filterByEventDF("payload")("comment")),
          filterByEventDF.select("created_at"),
          filterByEventDF.select(filterByEventDF("actor")("login")),
          filterByEventDF.select(filterByEventDF("repo")("name"))
        )
      }
      /*case "issuecommentevent" => {
        IssueCommentEventData(
          eventType,
          filterByEventDF.select(filterByEventDF("payload")("comment")),
          filterByEventDF.select("created_at"),
          filterByEventDF.select(filterByEventDF("repo")("name")),
          filterByEventDF.select(filterByEventDF("payload")("issue")),
          filterByEventDF.select("org")
        )
      }*/
      case ISSUES_EVENT.toLowerCase => {
        IssueEventData(
          eventType,
          filterByEventDF.select("created_at"),
          filterByEventDF.select(filterByEventDF("repo")("name")),
          filterByEventDF.select(filterByEventDF("payload")("issue")),
          filterByEventDF.select("org")
        )
      }
      case PULL_REQUEST_EVENT.toLowerCase => {
        PullRequestEventData(
          eventType,
          filterByEventDF.select("created_at"),
          filterByEventDF.select(filterByEventDF("repo")("name")),
          filterByEventDF.select(filterByEventDF("payload")("pull_request")("user")("login")),
          filterByEventDF.select(filterByEventDF("payload")("pull_request")("head")("repo")),
          filterByEventDF.select("org")
        )
      }
      case PUSH_EVENT.toLowerCase => {
        PushEventData(
          eventType,
          filterByEventDF.select("created_at"),
          filterByEventDF.select(filterByEventDF("repo")("name")),
          filterByEventDF.select(filterByEventDF("actor")("login")),
          filterByEventDF.select(filterByEventDF("payload")("commits"))
        )
      }
      case WATCH_EVENT.toLowerCase =>
        WatchEventData(
          eventType,
          filterByEventDF.select("created_at"),
          filterByEventDF.select(filterByEventDF("repo")("name")),
          filterByEventDF.select("org")
        )
    }
  } */
}
