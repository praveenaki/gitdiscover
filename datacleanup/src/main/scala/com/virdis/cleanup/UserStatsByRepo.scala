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

    val combinedDF = df.select(
      df(REPO_NAME_COLUMN).as(REPOSTATS_NAME),
      df(USER_LOGIN_COLUMN).as(USER_REPO_USERNAME_COLUMN),
      df(EVENT_TYPE)

    ).persist()


    val res = combinedDF.map {
      row =>
        ((row.getAs[String](REPOSTATS_NAME),
          row.getAs[String](USER_REPO_USERNAME_COLUMN),
          row.getAs[String](EVENT_TYPE)), 1L)
    }.groupByKey()


    val rows = res.map {
      r =>
        Row(
          r._1._1,
          r._1._2,
          r._1._3,
          r._2.toList.sum[Long]
        )
    }

    val finalRes = sQLContext.createDataFrame(rows, new StructType(
      Array(
        StructField("projectname", StringType),
        StructField("username", StringType),
        StructField("eventtype", StringType),
        StructField("count", LongType)
      )
    ))

    finalRes.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "useractivity", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }

  def useractivity(implicit sQLContext: SQLContext) = {
    S3_FILENAMES.zipWithIndex.foreach {
      case(_, idx) => userStatsByRepoName(s3FileHandle(idx))
    }
  }
}
