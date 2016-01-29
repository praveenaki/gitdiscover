package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.{GroupedData, DataFrame}
/**
  * Created by sandeep on 1/27/16.
  */

case class UserStats(eventType: String, counter: Long)

trait UserStatsByRepo {

  def userStatsByRepoName(df: DataFrame, reponame: String, login: String): Long = {
    ALL_EVENTS.foldLeft(0L)((b,a) => countByEvent(df, reponame, login, a))
  }

  def countByEvent(df: DataFrame, reponame: String, login: String, eventType: String): Long = {
    df.filter(df(REPO_NAME_COLUMN) === reponame && df(USER_LOGIN_COLUMN) === login && df(EVENT_TYPE) === eventType).count()

  }
}
