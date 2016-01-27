package com.virdis.cleanup

import org.apache.spark.sql.DataFrame
import Constants._
/**
  * Created by sandeep on 1/25/16.
  */
trait RepoTimeSeries {
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
      commitCommentEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      commitCommentEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      commitCommentEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER),
      commitCommentEventsDF(COMMIT_COMMENT_COLUMN).as(REPOSTATS_COMMENTS)
    ).join(repoNameLangEventDF, NAME_COLUMN)


    val issuesRepoSchema = issuesEventsDF.filter(issuesEventsDF(ISSUES_COMMENT_COLUMN).isNotNull).select(
      issuesEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      issuesEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      issuesEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      issuesEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_NAME),
      issuesEventsDF(ISSUES_COMMENT_COLUMN)as(REPOSTATS_COMMENTS)
    ).join(repoNameLangEventDF, NAME_COLUMN)

    val pullReqsRepoSchema = pullReqsEventsDF.filter(pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN).isNotNull)
        .filter(pullReqsEventsDF(PULL_REQ_COMMENT_COLUMN).isNotNull).select(
      pullReqsEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      pullReqsEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      pullReqsEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      pullReqsEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER),
      pullReqsEventsDF(PULL_REQ_COMMENT_COLUMN).as(REPOSTATS_COMMENTS),
      pullReqsEventsDF(PULL_REQ_LANGUAGE_COLUMN)
    )

    // flatten push event comments
    val pushEventRepoSchema = pushEventsDF.select(
      pushEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      pushEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      pushEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      pushEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER),
      pushEventsDF(PUSH_EVENT_COMMENT_COLUMN).as(REPOSTATS_COMMENTS)
    ).join(repoNameLangEventDF, NAME_COLUMN)


    val watchEventsRepoSchema = watchEventsDF.select(
      watchEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      watchEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      watchEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      watchEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER)
    ).join(repoNameLangEventDF, NAME_COLUMN)

    val forkEventsRepoSchema = forkEventsDF.select(
      forkEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      forkEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      forkEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      forkEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER)
    ).join(repoNameLangEventDF, NAME_COLUMN)


    val issueCommentEventsRepoSchema = issueCommentEventsDF.select(
      issueCommentEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      issueCommentEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      issueCommentEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      issueCommentEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER),
      issueCommentEventsDF(ISSUE_COMMENT_EVENT_COLUMN).as(REPOSTATS_COMMENTS)
    ).join(repoNameLangEventDF, NAME_COLUMN)


    val pullReqCRRepoSchema = pullReqsCommentReviewEventsDF.select(
      pullReqsCommentReviewEventsDF(REPO_NAME_COLUMN).as(NAME_COLUMN),
      pullReqsCommentReviewEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_MONTH),
      pullReqsCommentReviewEventsDF(CREATED_AT_COLUMN).as(REPOSTATS_CREATEDAT),
      pullReqsCommentReviewEventsDF(USER_LOGIN_COLUMN).as(REPOSTATS_EVENT_COMMITTER),
      pullReqsCommentReviewEventsDF(PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN).as(REPOSTATS_COMMENTS)
    ).join(repoNameLangEventDF, NAME_COLUMN)
  }



}
