package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, Row, SQLContext, DataFrame}
/**
  * Created by sandeep on 1/27/16.
  */


trait UserStatsByRepo {

  self: CommonDataFunctions =>

  def userStatsByRepoName(df: DataFrame)(implicit sQLContext: SQLContext) = {
    val repoLangDF = repoAndLanguageDF(df)

    val combinedDF = df.select(
      df(REPO_NAME_COLUMN).as(REPOSTATS_NAME),
      df(USER_LOGIN_COLUMN).as(USER_REPO_USERNAME_COLUMN)
    ).join(repoLangDF.select(
      repoLangDF(REPO_NAME_COLUMN).as(REPOSTATS_NAME)
    ), REPOSTATS_NAME).persist()

    val activityCount = combinedDF.groupBy(combinedDF(REPOSTATS_NAME), combinedDF(USER_REPO_USERNAME_COLUMN)).count()

    activityCount.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "useractivity", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }

  def useractivity(implicit sQLContext: SQLContext) = {
    S3_FILENAMES.zipWithIndex.foreach {
      case(_, idx) => userStatsByRepoName(s3FileHandle(idx))
    }
  }
}
