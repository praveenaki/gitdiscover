package com.virdis.cleanup.models

import org.apache.spark.sql.DataFrame

/**
  * Created by sandeep on 1/19/16.
  */

sealed trait GitData

case class CommitCommentEventData(eventType: String,
                                  comment: DataFrame,
                                  createdAt: DataFrame,
                                  commitCreatorUserName: DataFrame,
                                  fullRepoName: DataFrame) extends GitData


case class IssueCommentEventData(eventType: String,
                                 comment: DataFrame,
                                 createdAt: DataFrame,
                                 fullRepoName: DataFrame,
                                 issue: DataFrame,
                                 org: DataFrame) extends GitData

case class IssueEventData(eventType: String,
                          createdAt: DataFrame,
                          fullRepoName: DataFrame,
                          issue: DataFrame,
                          org: DataFrame) extends GitData

/*
  has language and description
 */
case class PullRequestEventData(eventType: String,
                                createdAt: DataFrame,
                                fullRepoName: DataFrame,
                                pullReqCreatorUserName: DataFrame,
                                repo: DataFrame,
                                org: DataFrame) extends GitData

case class PushEventData(eventType: String,
                         createdAt: DataFrame,
                         fullRepoName: DataFrame,
                         pushEvtCreatorUserName: DataFrame,
                         commitList: DataFrame) extends GitData

case class WatchEventData(eventType: String,
                          createdAt: DataFrame,
                          fullRepoName: DataFrame,
                          org: DataFrame) extends GitData