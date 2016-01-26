package com.virdis.cleanup.models

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

/**
  * Created by sandeep on 1/19/16.
  */

sealed trait GitData

case class CommitCommentEventData(projectName: String,
                                  month: String,
                                  createdat: DateTime,
                                  eventType: String,
                                  comment: String,
                                  committer: String) extends GitData

