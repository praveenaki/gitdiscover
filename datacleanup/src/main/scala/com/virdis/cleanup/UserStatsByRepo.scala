package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.{GroupedData, DataFrame}
/**
  * Created by sandeep on 1/27/16.
  */
trait UserStatsByRepo {

  val ALL_EVENTS = List(
                          COMMIT_COMMENT_EVENT, ISSUES_EVENT,
                          PUSH_EVENT, PULL_REQUEST_EVENT, WATCH_EVENT, FORK_EVENT,
                          ISSUE_COMMENT_EVENT, PULL_REQUEST_COMMENT_REVIEW_EVENT
  )

  def userStatsByRepoName(df: DataFrame, reponame: String, login: String): Long = {
    ALL_EVENTS.foldLeft(0L)((b,a) => countByEvent(df, reponame, login, a))
  }

  def countByEvent(df: DataFrame, reponame: String, login: String, eventType: String) = {
    df.filter(df(REPO_NAME_COLUMN) === reponame
      && df(USER_LOGIN_COLUMN) === login && df(EVENT_TYPE) === eventType).count()

  }
}
