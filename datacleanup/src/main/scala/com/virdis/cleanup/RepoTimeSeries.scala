package com.virdis.cleanup

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Row, SaveMode, DataFrame}
import Constants._
import org.joda.time.DateTime

/**
  * Created by sandeep on 1/25/16.
  */
trait RepoTimeSeries {
  self: DataManipulator =>

  def extractAndSaveProjectDetails(df: DataFrame) = {

    val pullReqsEventsDF = getDataByEventType(df, PULL_REQUEST_EVENT)

    val repoNameLangEventDF = pullReqsEventsDF.filter(pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN).isNotNull).select(
      pullReqsEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN)
    )

    val tsColsDF = df.select(
      df(REPO_NAME_COLUMN).as(NAME_COLUMN),
      df(CREATED_AT_COLUMN).cast(DateType).as(REPOSTATS_CREATEDAT),
      df(EVENT_TYPE).as(REPOSTATS_EVENT_TYPE),
      df(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER)
    ).join(repoNameLangEventDF, NAME_COLUMN)

    tsColsDF.map {
      row =>
        Row(
          row.getAs[String](0)
        )
    }

  }



}
