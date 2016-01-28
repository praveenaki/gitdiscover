package com.virdis.cleanup

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import Constants._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
/**
  * Created by sandeep on 1/20/16.
  */
trait TopProjectQuery {
  self: DataManipulator =>

  def countEventsByRepo(eventType: String, df: DataFrame) = {
    getDataByEventType(df, eventType).groupBy(REPO_NAME_COLUMN).agg(count(REPO_NAME_COLUMN).alias(TOTAL_COLUMN))
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

    pullReqs.filter(pullReqs(PULL_REQ_LANGUAGE_COLUMN).isNotNull).select(pullReqs(REPO_NAME_COLUMN),
      pullReqs(PULL_REQ_LANGUAGE_COLUMN))
      .groupBy("language","name").agg( count("language").as("pulltotals") )

  }

  def topProjectsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {

    val total = countOfSimilarStructureEventsByRepo(df).join(pullReqsByLangRepo(df), "name").map {
      row =>
        Row(DATE_FORMAT.print(TODAY),
          row.getAs[String](0), row.getAs[String](2), row.getAs[Long](1) + row.getAs[Long](3) )
    }
    sqlContext.createDataFrame(total, new StructType(
      Array(
        StructField("date", StringType),
        StructField("name", StringType),
        StructField("language", StringType),
        StructField("eventstotal", LongType)
      )
    ))
  }
  //[name, date, lang, event, date, lang, event]
  def mergeDataFrames(df1: DataFrame, df2: DataFrame)(implicit sQLContext: SQLContext): DataFrame = {
    val merged = df1.join(df2, NAME_COLUMN).map {
      row =>
        Row(
          row.getAs[String](1),
          row.getAs[String](0),
          row.getAs[String](2),
          row.getAs[Long](3) + row.getAs[Long](6)
        )
    }
    sQLContext.createDataFrame(merged, new StructType(
      Array(
        StructField("date", StringType),
        StructField("name", StringType),
        StructField("language", StringType),
        StructField("eventstotal", LongType)
      )
    ))
  }

  def topProjects(implicit sqlContext: SQLContext) = {
    val df1 = sqlContext.read.json(S3_FILENAMES(0))
    val df2 =  sqlContext.read.json(S3_FILENAMES(1))
    val df3 =  sqlContext.read.json(S3_FILENAMES(2))
    val df4 =  sqlContext.read.json(S3_FILENAMES(3))
    val df5 =  sqlContext.read.json(S3_FILENAMES(4))
    val df6 =  sqlContext.read.json(S3_FILENAMES(5))
    val df7 =  sqlContext.read.json(S3_FILENAMES(6))
    val df8 =  sqlContext.read.json(S3_FILENAMES(7))
    val df9 =  sqlContext.read.json(S3_FILENAMES(8))
    val df10 =  sqlContext.read.json(S3_FILENAMES(9))
    val df11 =  sqlContext.read.json(S3_FILENAMES(10))
    val df12 =  sqlContext.read.json(S3_FILENAMES(11))

    val res12 = mergeDataFrames(topProjectsByLangRepo(df1), topProjectsByLangRepo(df2))
    val res34 = mergeDataFrames(topProjectsByLangRepo(df3), topProjectsByLangRepo(df4))
    val res56 = mergeDataFrames(topProjectsByLangRepo(df5), topProjectsByLangRepo(df6))
    val res78 = mergeDataFrames(topProjectsByLangRepo(df7), topProjectsByLangRepo(df8))
    val res910 = mergeDataFrames(topProjectsByLangRepo(df9), topProjectsByLangRepo(df10))
    val res1112 = mergeDataFrames(topProjectsByLangRepo(df11), topProjectsByLangRepo(df12))

    // merge results
    val mr1 = mergeDataFrames(res12, res34)
    val mr2 = mergeDataFrames(res56, res78)
    val mr3 = mergeDataFrames(res910, res1112)

    val rz = mergeDataFrames(mr1, mr2)
    mergeDataFrames(rz, mr3)
  }



}
