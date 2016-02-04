package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, Row, SQLContext, DataFrame}
/**
  * Created by sandeep on 1/27/16.
  */


trait UserStatsByRepo {

  self: CommonDataFunctions =>

  def commonColumns(df: DataFrame): DataFrame = {
    df.select(
      df(REPO_NAME_COLUMN).as(REPOSTATS_NAME),
      df(USER_LOGIN_COLUMN).as(USER_REPO_USERNAME_COLUMN),
      df(EVENT_TYPE)

    ).persist()

  }

  def userStatsByRepoName(filteredDF: DataFrame)(implicit sQLContext: SQLContext): DataFrame = {



    val res = filteredDF.map {
      row =>
        ((row.getAs[String](REPOSTATS_NAME),
          row.getAs[String](USER_REPO_USERNAME_COLUMN),
          row.getAs[String](EVENT_TYPE)), 1L)
    }.groupByKey()


    val rows = res.map {
      val id = java.util.UUID.randomUUID().toString
      r =>
        Row(
          r._1._1,
          r._1._2,
          r._1._3,
          r._2.toList.sum[Long],
          id
        )
    }

    sQLContext.createDataFrame(rows, new StructType(
      Array(
        StructField("projectname", StringType),
        StructField("username", StringType),
        StructField("eventtype", StringType),
        StructField("count", LongType),
        StructField("id", StringType)
      )
    )).persist()


  }

  def useractivity(implicit sQLContext: SQLContext) = {
    val allDFs = (0 until 12).toList.map(i => commonColumns(s3FileHandle(i)))
    val finalRes = allDFs.reduce(_ unionAll(_))

    userStatsByRepoName(finalRes).write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "useractivity", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }
}
