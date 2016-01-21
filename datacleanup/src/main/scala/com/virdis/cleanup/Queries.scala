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


  def totalIssuePushWatchEventsByRepo(df: DataFrame)(implicit sQLContext: SQLContext): DataFrame = {
    val pushEventsByRepo = countEventsByRepo(PUSH_EVENT, df)
    val watchEventsByRepo = countEventsByRepo(WATCH_EVENT, df)
    val issueEventsByRepo = countEventsByRepo(ISSUES_EVENT, df)
    val sumUpRow = pushEventsByRepo.join(watchEventsByRepo, "name").map {
      row =>
        Row(row.getString(0), row.getAs[Long](1) + row.getAs[Long](2))
    }
    val sumPushWatchDF = sQLContext.createDataFrame(sumUpRow, new StructType(
      Array(
        StructField("name", StringType),
        StructField("total", LongType)
      )
    ))

    val sumAllIPWEvent = issueEventsByRepo.join(sumPushWatchDF, "name").map {
      row =>
        Row(row.getString(0), row.getAs[Long](1) + row.getAs[Long](2))
    }
    sQLContext.createDataFrame(sumAllIPWEvent,  new StructType(
      Array(
        StructField("name", StringType),
        StructField("ipwTotals", LongType)
      )
    ))
  }

  def pullReqsByLangRepo(df: DataFrame)(implicit sQLContext: SQLContext): DataFrame = {
    val pullReqs = getDataByEventType(df, PULL_REQUEST_EVENT)
    pullReqs.select(pullReqs("repo.name"),
      pullReqs("payload.pull_request.base.repo.language"))

    val nonNullPullReqs =   pullReqs.filter(pullReqs("language").isNotNull)
              .groupBy("language","name").agg( count("language").as("pullTotals") )
    nonNullPullReqs

  }

  def joinAcrossEventsByLangRepo(df: DataFrame)(implicit sQLContext: SQLContext): DataFrame = {
    val total = totalIssuePushWatchEventsByRepo(df).join(pullReqsByLangRepo(df), "name").map {
      row =>
        Row(row.getString(0), row.getString(2), row.getAs[Long](1) + row.getAs[Long](3) )
    }
    sQLContext.createDataFrame(total, new StructType(
      Array(
        StructField("name", StringType),
        StructField("language", StringType),
        StructField("eventsTotal", LongType)
      )
    )).sort(desc("eventsTotal"))
  }

}
