package com.virdis.cleanup

import org.apache.spark.sql.DataFrame
import Constants._
/**
  * Created by sandeep on 1/25/16.
  */
trait TimeSeries {
  self: DataManipulator =>

  def extractProjectDetails(df: DataFrame) = {
    val commitCommentEventsDF = getDataByEventType(df, COMMIT_COMMENT_EVENT)
    val issuesEventsDF = getDataByEventType(df, ISSUES_EVENT)
    val pullReqsEventsDF = getDataByEventType(df, PULL_REQUEST_EVENT)
    val pushEventsDF = getDataByEventType(df, PUSH_EVENT)
    val watchEventsDF = getDataByEventType(df, WATCH_EVENT)
    val forkEventsDF = getDataByEventType(df, FORK_EVENT)
    val issueCommentEventsDF = getDataByEventType(df, ISSUE_COMMENT_EVENT)
    val pullReqsCommentReviewEventsDF = getDataByEventType(df, PULL_REQUEST_COMMENT_REVIEW)

    val repoNameLangEventDF = pullReqsEventsDF.filter(pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN).isNotNull).select(
      pullReqsEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN)
    )

    val cceRepoSchema = commitCommentEventsDF.filter(commitCommentEventsDF(COMMIT_COMMENT_COLUMN).isNotNull).select(
      commitCommentEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      commitCommentEventsDF(CREATED_AT_COLUMN).as(CCE_MONTH),
      commitCommentEventsDF(CREATED_AT_COLUMN).as(CCE_TIME),
      commitCommentEventsDF(USER_LOGIN_COLUMN).as(CCE_COMMITTER),
      commitCommentEventsDF(COMMIT_COMMENT_COLUMN).as(CCE_COMMENT)
    ).join(repoNameLangEventDF, NAME_COLUMN)


    val issuesRepoSchema = issuesEventsDF.filter(issuesEventsDF(ISSUES_COMMENT_COLUMN).isNotNull).select(
      issuesEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      issuesEventsDF(CREATED_AT_COLUMN).as(I_MONTH),
      issuesEventsDF(CREATED_AT_COLUMN).as(I_TIME),
      issuesEventsDF(USER_LOGIN_COLUMN).as(I_COMMITTER),
      issuesEventsDF(ISSUES_COMMENT_COLUMN)as(I_COMMENT)
    ).join(repoNameLangEventDF, NAME_COLUMN)

    val pullReqsRepoSchema = pullReqsEventsDF.filter(pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN).isNotNull)


  }
}
