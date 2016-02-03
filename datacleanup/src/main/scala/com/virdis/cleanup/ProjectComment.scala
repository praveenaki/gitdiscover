package com.virdis.cleanup

import com.virdis.cleanup.Constants._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions.explode

/**
  * Created by sandeep on 2/2/16.
  */
trait ProjectComment {

  self: CommonDataFunctions =>

  def extractComments(df: DataFrame)(implicit sQLContext: SQLContext) = {

    val commitEvnts = getDataByEventType(df, COMMIT_COMMENT_EVENT)
    val commitCommentsDF = commitEvnts.select(
      commitEvnts(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
      commitEvnts(COMMIT_COMMENT_COLUMN).as(PROJECT_COMMENTS_COMMENT_COLUMN)
    ).persist()

    updateDataframeAndSave(commitCommentsDF)

    val pullReqCommentReviewEvent =  getDataByEventType(df, PULL_REQUEST_COMMENT_REVIEW_EVENT)

    val pullRCRFirstDF = pullReqCommentReviewEvent
         .filter(pullReqCommentReviewEvent(PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN).isNotNull)
         .select(pullReqCommentReviewEvent(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
          pullReqCommentReviewEvent(PULL_REQ_COMMENT_REVIEW_COMMENT_COLUMN).as(PROJECT_COMMENTS_COMMENT_COLUMN)
         ).persist()

    updateDataframeAndSave(pullRCRFirstDF)

    val pullRERSecondDF = pullReqCommentReviewEvent
      .filter(pullReqCommentReviewEvent("payload.pull_request.body").isNotNull)
      .select(pullReqCommentReviewEvent(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
        pullReqCommentReviewEvent("payload.pull_request.body").as(PROJECT_COMMENTS_COMMENT_COLUMN)
      ).persist()

    updateDataframeAndSave(pullRERSecondDF)

    val issueCommentEvents = getDataByEventType(df, ISSUE_COMMENT_EVENT)

    val issueCommentEvntFirstDF = issueCommentEvents
      .select(
        issueCommentEvents(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
        issueCommentEvents(ISSUES_COMMENT_COLUMN).as(PROJECT_COMMENTS_COMMENT_COLUMN)
      ).persist()

    updateDataframeAndSave(issueCommentEvntFirstDF)


    val issueCommentEvntSecondDF = issueCommentEvents.filter(issueCommentEvents("payload.comment.body").isNotNull)
                                    .select(
                                      issueCommentEvents(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
                                      issueCommentEvents("payload.comment.body").as(PROJECT_COMMENTS_COMMENT_COLUMN)
                                    ).persist()

    updateDataframeAndSave(issueCommentEvntSecondDF)

    val prEvent = getDataByEventType(df, PULL_REQUEST_EVENT)
    val pullReqEventsDF = prEvent.select(
      prEvent(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
      prEvent(PULL_REQ_COMMENT_COLUMN ).as(PROJECT_COMMENTS_COMMENT_COLUMN)
    ).persist()

    updateDataframeAndSave(pullReqEventsDF)

    val pushEvents = getDataByEventType(df, PUSH_EVENT)
    val pushEvtsDF = pushEvents.select(
      pushEvents(REPO_NAME_COLUMN).as(PROJECT_COMMENTS_PROJ_NAME_COLUMN),
      pushEvents("payload.commits")
    ).persist()


    /*
        Flatten exploded comments
     */
    val allPEComments = pushEvtsDF.explode("commits.message", PROJECT_COMMENTS_COMMENT_COLUMN){s: Seq[String] => s}

    updateDataframeAndSave(allPEComments)

  }

  def updateDataframeAndSave(df: DataFrame)(implicit sQLContext: SQLContext) = {
    val ir = df.map {
      val srt =  java.util.UUID.randomUUID().toString

      row =>
        Row(
          row.getAs[String](PROJECT_COMMENTS_PROJ_NAME_COLUMN),
          row.getAs[String](PROJECT_COMMENTS_COMMENT_COLUMN),
          srt
        )
    }
    sQLContext.createDataFrame(ir, new StructType(
      Array(
        StructField(PROJECT_COMMENTS_PROJ_NAME_COLUMN, StringType),
        StructField(PROJECT_COMMENTS_COMMENT_COLUMN, StringType),
        StructField("sorter", StringType)
      )
    )).write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "projectcomments", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }

  def projectComments(implicit sQLContext: SQLContext) = {
    S3_FILENAMES.zipWithIndex.foreach {
      case(_, idx) => extractComments(s3FileHandle(idx))
    }
  }
}