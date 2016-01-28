package com.virdis.cleanup

import org.apache.spark.sql.types.{StructField, StructType, StringType, DateType}
import org.apache.spark.sql.{SQLContext, Row, SaveMode, DataFrame}
import Constants._
import org.joda.time.DateTime

/**
  * Created by sandeep on 1/25/16.
  */

/*
    This should be run for top 15 projects
 */
trait RepoTimeSeries {
  self: DataManipulator =>

  def extractAndSaveRepoStats(df: DataFrame)(implicit sQLContext: SQLContext) = {

    val pullReqsEventsDF = getDataByEventType(df, PULL_REQUEST_EVENT)

    val repoNameLangEventDF = pullReqsEventsDF.filter(pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN).isNotNull).select(
      pullReqsEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN).as(REPOSTATS_LANGUAGE)
    )

    val tsColsDF = df.select(
      df(REPO_NAME_COLUMN).as(NAME_COLUMN),
      df(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      df(EVENT_TYPE).as(REPOSTATS_EVENT_TYPE),
      df(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER)
    ).join(repoNameLangEventDF, NAME_COLUMN)

    val ir = tsColsDF.map {
      row =>
        Row(
          row.getAs[String](0),
          DateTime.parse(row.getAs[String](1)).toString("MMM"),
          DateTime.parse(row.getAs[String](1)).toString(DATE_FORMAT),
          row.getAs[String](2),
          row.getAs[String](3),
          row.getAs[String](4)
        )
    }

    val res = sQLContext.createDataFrame(ir, new StructType(
      Array(
        StructField("projectname", StringType),
        StructField("month", StringType),
        StructField("createdat", StringType),
        StructField("eventtype", StringType),
        StructField("eventcommitter", StringType),
        StructField("language", StringType)
      )
    ))

  }

}
