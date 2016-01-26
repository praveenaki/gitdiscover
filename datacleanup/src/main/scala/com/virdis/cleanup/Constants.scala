package com.virdis.cleanup

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

  val CCE_REPO_NAME = "ccereponame"
  val CCE_TIME = "ccedate"
  val CCE_MONTH = "ccemonth"
  val CCE_CREATED_AT = "ccecreatedat"
  val CCE_COMMITTER = "ccelogin"
  val CCE_COMMENT = "ccecomment"

  val I_MONTH = "imonth"
  val I_TIME = "itime"
  val I_COMMITTER = "ilogin"
  val I_COMMENT ="icomment"
  ///// SCHEMA /////
  val CREATED_DATE_TIME = DateTimeFormat.forPattern("")
}
