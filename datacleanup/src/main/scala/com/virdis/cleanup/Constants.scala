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
  val PULL_REQUEST_COMMENT_REVIEW_EVENT = "PullRequestReviewCommentEvent"

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
  val EVENT_TYPE = "type"
  val LANGUAGE_COLUMN = "language"

  val REPOSTATS_NAME = "projectname"
  val REPOSTATS_MONTH = "month"
  val REPOSTATS_CREATEDAT = "createdat"
  val REPOSTATS_LANGUAGE = "language"
  val REPOSTATS_EVENT_TYPE = "eventtype"
  val REPOSTATS_COMMENTS = "comments"
  val REPOSTATS_EVENT_COMMITTER = "eventcommitter"

  val PUSH_COUNT = "pushcount"
  val PULL_COUNT = "pullcount"
  val COMMIT_COUNT = "commitcount"
  val WATCH_COUNT = "watchcount"
  val ISSUE_COUNT = "issuecount"
  val FORK_COUNT = "forkcount"
  val ISSUE_COMMENT_EVENT_COUNT = "issuecmtcount"
  val PULL_COMMENT_REVIEW_COUNT = "pullcmtrevcount"

  val TOPREPOS_NAME_COLUMN = "name"
  val TOPREPOS_EVENTSTOTAL_COLUMN = "eventstotal"
  val TOPREPOS_DATE_COLUMN = "date"
  val TOPREPOS_LANGUAGE_COLUMN = "language"
  val TOPREPOS_EVENTTOTAL_SUM_COLUMN = "sum(eventstotal)"

  val USER_REPO_STATS_NAME_COLUMN = "projectname"
  val USER_REPO_USERNAME_COLUMN = "username"
  val USER_REPO_EVENT_TYPE = "eventttype"
  val USER_REPO_ACTIVITY_COUNT = "count"

  val PROJECT_COMMENTS_PROJ_NAME_COLUMN = "projectname"
  val PROJECT_COMMENTS_COMMENT_COLUMN = "comment"

  ///// SCHEMA /////
 // val CREATED_DATE_TIME = DateTimeFormat.forPattern("MM/dd/yyyy")

  val BUCKET_PATH = "s3n://sandeep-git-archive/"

  val S3_FILENAMES = IndexedSeq(
    BUCKET_PATH + "JanFull.json",
    BUCKET_PATH + "FebFull.json",
    BUCKET_PATH + "MarFull.json",
    BUCKET_PATH + "AprilFull.json",
    BUCKET_PATH + "MayFull.json",
    BUCKET_PATH + "JuneFull.json",
    BUCKET_PATH + "JulFull.json",
    BUCKET_PATH + "AugFull.json",
    BUCKET_PATH + "SepFull.json",
    BUCKET_PATH + "OctFull.json",
    BUCKET_PATH + "NovFull.json",
    BUCKET_PATH + "DecFull.json"
  )

  val ALL_EVENTS = List(
    COMMIT_COMMENT_EVENT, ISSUES_EVENT,
    PUSH_EVENT, PULL_REQUEST_EVENT, WATCH_EVENT, FORK_EVENT,
    ISSUE_COMMENT_EVENT, PULL_REQUEST_COMMENT_REVIEW_EVENT
  )


}
