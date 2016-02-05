package com.virdis.cleanup

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, Row, SQLContext}
import Constants._
/**
  * Created by sandeep on 2/1/16.
  */
trait EdgeAcrossProjects {

  val userActivityRepo = "useractivityrepo"
  val username = "username"
  val projName = "projectname"
  val eventcommitter = "eventcommitter"
  val eventype = "eventtype"

  def findEdge(implicit sQLContext: SQLContext) = {

    val repostatsDF = sQLContext.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "repostats", "keyspace" -> "git" ))
      .load()

    val userActivityDF = sQLContext.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "useractivity", "keyspace" -> "git" ))
      .load()


    val filteredRSDF = repostatsDF.select(
                          repostatsDF(projName),
                          repostatsDF(eventcommitter).as(username),
                          repostatsDF(eventype)
                        )

   val filteredUADF = userActivityDF.select(
                         userActivityDF("projectname").as(userActivityRepo),
                         userActivityDF(username),
                         userActivityDF("count")
                       )


    val joinedRepo = filteredRSDF.filter(
      !filteredRSDF(projName).contains(username) &&
        filteredRSDF(eventype) === PULL_REQUEST_EVENT &&
        filteredRSDF(eventype) === COMMIT_COMMENT_EVENT &&
        filteredRSDF(eventype) === PUSH_EVENT &&
        filteredRSDF(eventype) === ISSUE_COMMENT_EVENT
    )
      .join(
          filteredUADF.filter(!filteredUADF(userActivityRepo).contains(filteredUADF(username))), username
      ).persist()

    val ir = joinedRepo.map {
      val id = java.util.UUID.randomUUID()
      row =>
        Row(
          row.getAs[String](username),
          row.getAs[String](projName),
          row.getAs[String](eventype),
          row.getAs[String](userActivityRepo),
          row.getAs[Long]("count"),
          id
        )
    }

    val finalResult = sQLContext.createDataFrame(ir, new StructType(
      Array(
        StructField(username, StringType),
        StructField(projName, StringType),
        StructField(eventype, StringType),
        StructField(userActivityRepo, StringType),
        StructField("count", LongType),
        StructField("sorter", StringType)
      )
    ))

    finalResult.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "edgeinfo", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }

}
