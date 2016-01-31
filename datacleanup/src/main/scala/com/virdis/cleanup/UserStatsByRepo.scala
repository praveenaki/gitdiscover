package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.{SQLContext, GroupedData, DataFrame}
/**
  * Created by sandeep on 1/27/16.
  */

case class UserStats(eventType: String, counter: Long)

trait UserStatsByRepo {

  self: CommonDataFunctions =>

  def userStatsByRepoName(df: DataFrame, reponame: String, login: String)(implicit sQLContext: SQLContext): Long = {
    val jrez = joinDataWithTopRepos(df)
    val userActivity = ALL_EVENTS.foldLeft(0L)((b,a) => countByEvent(jrez, reponame, login, a))

  }

  def countByEvent(df: DataFrame, reponame: String, login: String, eventType: String): Long = {
    df.filter(df(REPO_NAME_COLUMN) === reponame && df(USER_LOGIN_COLUMN) === login && df(EVENT_TYPE) === eventType).count()

  }

  def joinDataWithTopRepos(df: DataFrame)(implicit sQLContext: SQLContext) = {
    val top200NameAndLang = getData(sQLContext)

    val selectedColsDF = df.select(
      df(REPO_NAME_COLUMN).as(NAME_COLUMN), df(USER_LOGIN_COLUMN), df(EVENT_TYPE)
    )
    selectedColsDF.join(top200NameAndLang, NAME_COLUMN)
  }

}
