package com.virdis.cleanup

import org.apache.spark.sql.{SaveMode, Row, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import Constants._
/**
  * Created by sandeep on 1/20/16.
  */
trait TopProjectQuery {
  self: DataManipulator =>

  def countEventsByRepo(eventType: String, df: DataFrame) = {
    getDataByEventType(df, eventType).groupBy(REPO_NAME_COLUMN).agg(count(REPO_NAME_COLUMN).alias(TOTAL_COLUMN))
  }


  def repoEventCount(df: DataFrame)(implicit sQLContext: SQLContext) = {
    val repoNameAndLang = pullReqsByLangRepo(df)(sQLContext)

    val completeDFWithLang = df.join(repoNameAndLang).where(df("repo.name") === repoNameAndLang("name"))

    completeDFWithLang.groupBy(REPO_NAME_COLUMN, LANGUAGE_COLUMN).agg( count(EVENT_TYPE).as("eventstotal") )
  }

  def pullReqsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val pullReqs = getDataByEventType(df, PULL_REQUEST_EVENT)

    pullReqs.filter(pullReqs(PULL_REQ_LANGUAGE_COLUMN).isNotNull).select(pullReqs(REPO_NAME_COLUMN),
      pullReqs(PULL_REQ_LANGUAGE_COLUMN))
  }

  def topProjectsByLangRepo(df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {

    val stats = repoEventCount(df).map {
      row =>
        Row(
          DateTimeFormat.forPattern("MM/dd/yyyy").print(DateTime.now),
          row.getAs[String](0),
          row.getAs[String](1),
          row.getAs[Long](2)
        )
    }

    sqlContext.createDataFrame(stats, new StructType(
      Array(
        StructField("date", StringType),
        StructField("name", StringType),
        StructField("language", StringType),
        StructField("eventstotal", LongType)
      )
    ))
  }

  /*
      lazy load files from S3
   */

  def unionResult(idx1: Int, idx2: Int)(implicit sQLContext: SQLContext): DataFrame = {
    val res1 = topProjectsByLangRepo(s3FileHandle(idx1))
    val res2 = topProjectsByLangRepo(s3FileHandle(idx2))
    res1.unionAll(res2)
  }

  def topProjects(implicit sqlContext: SQLContext) = {
    val res12 = unionResult(0, 1)

    val res34 = unionResult(2,3)

    val res1234 = res12.unionAll(res34)

    val res56 = unionResult(4,5)

    val res78 = unionResult(6,7)

    val res5678 = res56.unionAll(res78)

    val res910 = unionResult(8,9)

    val res1112 = unionResult(10,11)

    val res912 = res910.unionAll(res1112)

    val inter = res1234.unionAll(res5678)

    val rez = inter.unionAll(res912)
    val grpRes =  rez.groupBy(TOPREPOS_NAME_COLUMN,TOPREPOS_EVENTSTOTAL_COLUMN,
      TOPREPOS_DATE_COLUMN_COLUMN,TOPREPOS_LANGUAGE_COLUMN).agg(sum(TOPREPOS_EVENTSTOTAL_COLUMN))

    grpRes.withColumnRenamed(TOPREPOS_EVENTTOTAL_SUM_COLUMN, TOPREPOS_EVENTSTOTAL_COLUMN).write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "toprepos", "keyspace" -> "git")).mode(SaveMode.Append).save()


  }

}
