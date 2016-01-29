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

  def topProjects(implicit sqlContext: SQLContext) = {
    val res1 = topProjectsByLangRepo(allDFS(sqlContext)(0))
    val res2 = topProjectsByLangRepo(allDFS(sqlContext)(1))
    val res12 = res1.unionAll(res2)
    val res3 = topProjectsByLangRepo(allDFS(sqlContext)(2))
    val res4 = topProjectsByLangRepo(allDFS(sqlContext)(3))
    val res34 = res3.unionAll(res4)

    val res1234 = res12.unionAll(res34)

    val res5 = topProjectsByLangRepo(allDFS(sqlContext)(4))
    val res6 = topProjectsByLangRepo(allDFS(sqlContext)(5))
    val res56 = res5.unionAll(res6)

    val res7 = topProjectsByLangRepo(allDFS(sqlContext)(6))
    val res8 = topProjectsByLangRepo(allDFS(sqlContext)(7))
    val res78 = res7.unionAll(res8)

    val res5678 = res56.unionAll(res78)

    val res9 = topProjectsByLangRepo(allDFS(sqlContext)(8))
    val res10 = topProjectsByLangRepo(allDFS(sqlContext)(9))
    val res910 = res9.unionAll(res10)

    val res11 = topProjectsByLangRepo(allDFS(sqlContext)(10))
    val res112 = topProjectsByLangRepo(allDFS(sqlContext)(11))
    val res1112 = res11.unionAll(res112)

    val res912 = res910.unionAll(res1112)

    val inter = res1234.unionAll(res5678)

    inter.unionAll(res912).write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "toprepos", "keyspace" -> "git")).mode(SaveMode.Append).save()


  }

}
