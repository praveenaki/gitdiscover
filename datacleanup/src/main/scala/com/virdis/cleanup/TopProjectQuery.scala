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
    lazy val res1 = topProjectsByLangRepo(allDFS(sqlContext)(0))
    lazy val res2 = topProjectsByLangRepo(allDFS(sqlContext)(1))
    lazy val res12 = res1.unionAll(res2)
    lazy val res3 = topProjectsByLangRepo(allDFS(sqlContext)(2))
    lazy val res4 = topProjectsByLangRepo(allDFS(sqlContext)(3))
    lazy val res34 = res3.unionAll(res4)

    lazy val res1234 = res12.unionAll(res34)

    lazy val res5 = topProjectsByLangRepo(allDFS(sqlContext)(4))
    lazy val res6 = topProjectsByLangRepo(allDFS(sqlContext)(5))
    lazy val res56 = res5.unionAll(res6)

    lazy val res7 = topProjectsByLangRepo(allDFS(sqlContext)(6))
    lazy val res8 = topProjectsByLangRepo(allDFS(sqlContext)(7))
    lazy val res78 = res7.unionAll(res8)

    lazy val res5678 = res56.unionAll(res78)

    lazy val res9 = topProjectsByLangRepo(allDFS(sqlContext)(8))
    lazy val res10 = topProjectsByLangRepo(allDFS(sqlContext)(9))
    lazy val res910 = res9.unionAll(res10)

    lazy val res11 = topProjectsByLangRepo(allDFS(sqlContext)(10))
    lazy val res112 = topProjectsByLangRepo(allDFS(sqlContext)(11))
    lazy val res1112 = res11.unionAll(res112)

    lazy val res912 = res910.unionAll(res1112)

    lazy val inter = res1234.unionAll(res5678)

    inter.unionAll(res912).write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "toprepos", "keyspace" -> "git")).mode(SaveMode.Append).save()


  }

}
