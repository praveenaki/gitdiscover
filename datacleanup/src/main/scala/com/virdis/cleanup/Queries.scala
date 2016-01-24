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
    getDataByEventType(df, eventType).groupBy(REPO_NAME_COLUMN_NAME).agg(count(REPO_NAME_COLUMN_NAME).alias(TOTAL_COLUMN))
  }

  def joinSimilarStructureReposByName(df1: DataFrame, df2: DataFrame, newColName: String)(implicit sqlContext: SQLContext): DataFrame = {
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


  def countOfSimilarStructureEventsByRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val pushEventsByRepo = countEventsByRepo(PUSH_EVENT, df)
    val watchEventsByRepo = countEventsByRepo(WATCH_EVENT, df)
    val issueEventsByRepo = countEventsByRepo(ISSUES_EVENT, df)
    val commitCommentsByRepo = countEventsByRepo(COMMIT_COMMENT_EVENT, df)
    val forksByRepo = countEventsByRepo(FORK_EVENT, df)
    val issueCommentsByRepo = countEventsByRepo(ISSUE_COMMENT_EVENT, df)

    val pushWatchDF = joinSimilarStructureReposByName(pushEventsByRepo, watchEventsByRepo, "total")
    val issuePushWatchDF = joinSimilarStructureReposByName(issueEventsByRepo, pushWatchDF, "ipwTotal")
    val cipwDF = joinSimilarStructureReposByName(commitCommentsByRepo, issuePushWatchDF, "cipwTotal")
    val fcipwDF = joinSimilarStructureReposByName(forksByRepo, cipwDF, "fcipwTotal")
    val icfcipwDF = joinSimilarStructureReposByName(issueCommentsByRepo, fcipwDF, "icfcipw")
    icfcipwDF
  }


  def pullReqsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val pullReqs = getDataByEventType(df, PULL_REQUEST_EVENT)

    pullReqs.select(pullReqs(REPO_NAME_COLUMN_NAME),
      pullReqs(LANGUAGE_COLUMN))
      .groupBy("language","name").agg( count("language").as("pulltotals") )

  }

  def topProjectsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val total = countOfSimilarStructureEventsByRepo(df).join(pullReqsByLangRepo(df), "name").map {
      row =>
        Row(row.getAs[String](0), row.getAs[String](2), row.getAs[Long](1) + row.getAs[Long](3) )
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
