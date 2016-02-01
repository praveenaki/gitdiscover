package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.{SaveMode, Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions._
/**
  * Created by sandeep on 1/27/16.
  */



case class UserRepoStats(projectname: String, username: String, eventtype: String, count: Long)

trait UserStatsByRepo {

  self: CommonDataFunctions =>

  def userStatsByRepoName(df: DataFrame)(implicit sQLContext: SQLContext) = {
    val repoLangDF = repoAndLanguageDF(df)

    val combinedDF = df.select(
      df(REPO_NAME_COLUMN).as(NAME_COLUMN),
      df(USER_LOGIN_COLUMN).as(USER_REPO_USERNAME_COLUMN),
      df(EVENT_TYPE).as(USER_REPO_EVENT_TYPE)
    ).join(repoLangDF, NAME_COLUMN).persist()


    val res = combinedDF.map {
      row =>
        ((row.getAs[String](NAME_COLUMN), row.getAs[String](USER_REPO_USERNAME_COLUMN), row.getAs[String](USER_REPO_EVENT_TYPE)), 1L)
    }.groupByKey()

    var stats = List.empty[UserRepoStats]
    res.foreach(r => stats = UserRepoStats(r._1._1, r._1._2, r._1._3, r._2.reduce(_ + _)) +: stats)

    sQLContext.createDataFrame(stats).write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "useractivity", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }

  def useractivity(implicit sQLContext: SQLContext) = {
    S3_FILENAMES.zipWithIndex.foreach {
      case(_, idx) => userStatsByRepoName(s3FileHandle(idx))
    }
  }
}
