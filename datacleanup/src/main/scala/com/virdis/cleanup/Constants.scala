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
  val PULL_REQ_COMMENT_COLUMN = "payload.pull_request.body"
  val PUSH_EVENT_COMMENT_COLUMN = "payload.commits"
  val ISSUE_COMMENT_EVENT_COLUMN = "payload.comment.body"
  val PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN = "payload.comment.body"

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

  val PR_MONTH = "pmonth"
  val PR_TIME = "prtime"
  val PR_COMMITTER = "prlogin"
  val PR_COMMENT = "prcomment"

  val PSH_MONTH = "pshmonth"
  val PSH_TIME = "pshtime"
  val PSH_COMMITTER = "pshlogin"
  val PSH_COMMENT = "pshcomment"

  val WATCH_MONTH = "wmonth"
  val WATCH_TIME = "wtime"
  val WATCH_COMMITTER = "wlogin"

  val FRK_MONTH ="frkmonth"
  val FRK_TIME = "frkmonth"
  val FRK_COMMITTER = "frklogin"

  val ISC_MONTH ="iscmonth"
  val ISC_TIME = "isctime"
  val ISC_COMMITTER  = "isclogin"
  val ISC_COMMENT = "isccomment"

  val PRCR_MONTH = "prcrmonth"
  val PRCR_TIME = "prcrtime"
  val PRCR_COMMITTER = "prcrlogin"
  val PRCR_COMMENT = "prcrcomment"

  ///// SCHEMA /////
  val CREATED_DATE_TIME = DateTimeFormat.forPattern("")
}
