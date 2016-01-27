package com.virdis.cleanup

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by sandeep on 1/19/16.
  */
object Constants {

  val COMMIT_COMMENT_EVENT = "CommitCommentEvent"
  val ISSUES_EVENT = "IssuesEvent"
  val PULL_REQUEST_EVENT = "PullRequestEvent"
  val PUSH_EVENT = "PushEvent"
  val WATCH_EVENT = "WatchEvent"
  val FORK_EVENT = "ForkEvent"
  val ISSUE_COMMENT_EVENT = "IssueCommentEvent"
  val PULL_REQUEST_COMMENT_REVIEW = "PullRequestReviewCommentEvent"

  ///// COLUMN NAMES /////
  val REPO_NAME_COLUMN = "repo.name"
  val NAME_COLUMN = "name"
  val TOTAL_COLUMN = "total"
  val PULL_REQ_LANGUAGE_COLUMN = "payload.pull_request.base.repo.language"
  val CREATED_AT_COLUMN = "created_at"
  val USER_LOGIN_COLUMN = "actor.login"
  val COMMIT_COMMENT_COLUMN = "payload.comment.body"
  val ISSUES_COMMENT_COLUMN = "payload.issue.body"
  val PULL_REQ_COMMENT_COLUMN = "payload.pull_request.body"
  val PUSH_EVENT_COMMENT_COLUMN = "payload.commits"
  val ISSUE_COMMENT_EVENT_COLUMN = "payload.comment.body"
  val PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN = "payload.comment.body"

  val REPOSTATS_NAME = "projectname"
  val REPOSTATS_MONTH = "month"
  val REPOSTATS_CREATEDAT = "createdat"
  val REPOSTATS_LANGUAGE = "language"
  val REPOSTATS_EVENT_TYPE = "eventtype"
  val REPOSTATS_COMMENTS = "comments"
  val REPOSTATS_EVENT_COMMITTER = "eventcommitter"
  ///// SCHEMA /////
 // val CREATED_DATE_TIME = DateTimeFormat.forPattern("MM/dd/yyyy")

  val BUCKET_PATH = "s3n://sandeep-git-archive/"
  val DATE_FORMAT = DateTimeFormat.forPattern("MM/dd/yyyy")
  val TODAY = DateTime.now
}
