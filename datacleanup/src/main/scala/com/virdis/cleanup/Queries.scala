package com.virdis.cleanup

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import Constants._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
/**
  * Created by sandeep on 1/20/16.
  */
trait Queries {
  self: DataManipulator =>

  /*
      Hypotheses to test: should sort after each join, or at the end. !!!!
   */
  def countEventsByRepo(eventType: String, df: DataFrame) = {
    getDataByEventType(df, eventType).groupBy("repo.name").agg(count("repo.name").alias("total"))
  }

  def joinReposByName(df1: DataFrame, df2: DataFrame, newColName: String)(implicit sqlContext: SQLContext): DataFrame = {
    val result = df1.join(df2, NAME_COLUMN).map {
      row =>
        Row(row.getString(0), row.getAs[Long](1) + row.getAs[Long](2))
    }
    sqlContext.createDataFrame(result, new StructType(
      Array(
        StructField("name", StringType),
        StructField(newColName, LongType)
      )
    ))

  }


  def totalIssuePushWatchEventsByRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val pushEventsByRepo = countEventsByRepo(PUSH_EVENT, df)
    val watchEventsByRepo = countEventsByRepo(WATCH_EVENT, df)
    val issueEventsByRepo = countEventsByRepo(ISSUES_EVENT, df)
    val commitCommentsByRepo = countEventsByRepo(COMMIT_COMMENT_EVENT, df)
    val forksByRepo = countEventsByRepo()
    val pushWatchDF = joinReposByName(pushEventsByRepo, watchEventsByRepo, "total")
    val issuePushWatchDF = joinReposByName(issueEventsByRepo, pushWatchDF, "ipwTotal")
    val cipwDF = joinReposByName(commitCommentsByRepo, issuePushWatchDF, "cipwTotal")

  }

  def pullReqsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val pullReqs = getDataByEventType(df, PULL_REQUEST_EVENT)
    pullReqs.select(pullReqs("repo.name"),
      pullReqs("payload.pull_request.base.repo.language"))
      .groupBy("language","name").agg( count("language").as("pullTotals") ).sort(desc("pullTotals"))

  }

  def joinAcrossEventsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val total = totalIssuePushWatchEventsByRepo(df).join(pullReqsByLangRepo(df), "name").map {
      row =>
        Row(row.getString(0), row.getString(2), row.getAs[Long](1) + row.getAs[Long](3) )
    }
    sqlContext.createDataFrame(total, new StructType(
      Array(
        StructField("name", StringType),
        StructField("language", StringType),
        StructField("eventstotal", LongType)
      )
    )).sort(desc("eventstotal"))
  }

}
