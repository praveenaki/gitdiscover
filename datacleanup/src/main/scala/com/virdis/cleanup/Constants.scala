package com.virdis.cleanup

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
  val REPO_NAME_COLUMN_NAME = "repo.name"
  val NAME_COLUMN = "name"
  val TOTAL_COLUMN = "total"
  val LANGUAGE_COLUMN = "payload.pull_request.base.repo.language"
}
