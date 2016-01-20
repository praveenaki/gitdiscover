package com.virdis.cleanup.models

import org.apache.spark.sql.DataFrame

/**
  * Created by sandeep on 1/19/16.
  */

sealed trait GitData
case class CommitCommentEventData(evntType: String,
                                  comment: DataFrame,
                                  createdAt: DataFrame,
                                  commitCreatorUserName: DataFrame,
                                  fullRepoName: DataFrame) extends GitData


