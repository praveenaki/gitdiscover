package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions.explode

/**
  * Created by sandeep on 2/2/16.
  */
trait ProjectComment {

  self: CommonDataFunctions =>

  def extractComments(df: DataFrame)(sQLContext: SQLContext) = {

    val commitEvnts = getDataByEventType(df, COMMIT_COMMENT_EVENT)
    val commitCommentsDF = commitEvnts.select(REPO_NAME_COLUMN,COMMIT_COMMENT_COLUMN).persist()

    val pullReqCommentReviewEvent =  getDataByEventType(df, PULL_REQUEST_COMMENT_REVIEW_EVENT)
    val pullRCRFirstDF = pullReqCommentReviewEvent
      .filter(pullReqCommentReviewEvent(PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN).isNotNull)
        .select(REPO_NAME_COLUMN, PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN).persist()

    val pullRERSecondDF = pullReqCommentReviewEvent
                            .filter(pullReqCommentReviewEvent("payload.pull_request.body").isNotNull)
                              .select(REPO_NAME_COLUMN, "payload.pull_request.body").persist()

    val issueCommentEvents = getDataByEventType(df, ISSUE_COMMENT_EVENT)

    val issueCommentEvntFirstDF = issueCommentEvents.select(REPO_NAME_COLUMN, ISSUES_COMMENT_COLUMN).persist()
    val issueCommentEvntSecondDF = issueCommentEvents.filter(issueCommentEvents("payload.comment.body").isNotNull)
                                    .select(REPO_NAME_COLUMN, "payload.comment.body").persist()



    val prEvent = getDataByEventType(df, PULL_REQUEST_EVENT)
    val pullReqEventsDF = prEvent.select( prEvent(REPO_NAME_COLUMN), prEvent(PULL_REQ_COMMENT_COLUMN ) ).persist()

    val pushEvents = getDataByEventType(df, PUSH_EVENT)
    val pushEvtsDF = pushEvents.select( pushEvents(REPO_NAME_COLUMN), pushEvents("payload.commits")).persist()

    val peComments = explode(pushEvtsDF("commits.message"))

  }
}