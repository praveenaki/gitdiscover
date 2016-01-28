package com.virdis.cleanup

import org.apache.spark.sql.functions._
import com.virdis.cleanup.Constants._
import org.apache.spark.sql.{GroupedData, DataFrame}
import org.apache.spark.sql.types._
/**
  * Created by sandeep on 1/27/16.
  */
trait UserStatsByRepo {

  def userStatsByRepoName(df: DataFrame, reponame: String) = {
    val filterRepoDF = df.filter(df(REPO_NAME_COLUMN) === reponame)
    val grpUserDF =  filterRepoDF.groupBy(USER_LOGIN_COLUMN)

    val pushEvtCount = countByEventType(grpUserDF, filterRepoDF, PUSH_EVENT, PUSH_COUNT)
    val pullEvtCount = countByEventType(grpUserDF, filterRepoDF, PULL_REQUEST_EVENT, PULL_COUNT)
    val commitEvtCount = countByEventType(grpUserDF, filterRepoDF, COMMIT_COMMENT_EVENT, COMMIT_COUNT)
    val watchEvtCount = countByEventType(grpUserDF, filterRepoDF, WATCH_EVENT, WATCH_COUNT)
    val issueEvntCount = countByEventType(grpUserDF, filterRepoDF, ISSUES_EVENT, ISSUE_COUNT)
    val forkEvntCount = countByEventType(grpUserDF, filterRepoDF,FORK_EVENT, FORK_COUNT)
    val issueCmtEventCount = countByEventType(grpUserDF, filterRepoDF, ISSUE_COMMENT_EVENT, ISSUE_COMMENT_EVENT_COUNT)
    val pullReqCmtCount = countByEventType(grpUserDF, filterRepoDF, PULL_REQUEST_COMMENT_REVIEW, PULL_COMMENT_REVIEW_COUNT)
  }

  def countByEventType(grpDF: GroupedData, df: DataFrame, evntType: String, renamedCol: String) = {
    grpDF.agg(
      sum( (df(EVENT_TYPE) === evntType).cast(IntegerType).as(renamedCol) )
    )
  }
}
