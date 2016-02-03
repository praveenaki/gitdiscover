package com.virdis.cleanup

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import Constants._
/**
  * Created by sandeep on 2/1/16.
  */
trait EdgeAcrossProjects {

  self: CommonDataFunctions =>

  val userActivityRepo = "useractivityrepo"
  val username = "username"
  val projName = "projectname"
  val eventcommitter = "eventcommitter"
  val eventype = "eventtype"
  val lang = "language"

  def findEdge(sQLContext: SQLContext) = {
    val repostatsDF = sQLContext.read
                      .format("org.apache.spark.sql.cassandra")
                      .options(Map( "table" -> "repostats", "keyspace" -> "git" ))
                      .load()

    val userActivityDF = sQLContext.read
                          .format("org.apache.spark.sql.cassandra")
                          .options(Map( "table" -> "useractivity", "keyspace" -> "git" ))
                          .load()

    val joinedRepo = repostatsDF.select(
                        repostatsDF(projName),
                        repostatsDF(eventcommitter).as(username),
                        repostatsDF(eventype),
                        repostatsDF(lang)
                      ).join(
                        userActivityDF.select(
                          userActivityDF("projectname").as(userActivityRepo),
                          userActivityDF(username),
                          userActivityDF("count")
                        ), username
                      ).persist()

    val filterdJoin = joinedRepo.filter( joinedRepo(projName) !== joinedRepo(userActivityRepo) ).persist()

    val ir = filterdJoin.map {
      row =>
        Row(
          row.getAs[String](username),
          row.getAs[String](projName),
          row.getAs[String](eventype),
          row.getAs[String](lang),
          row.getAs[String](userActivityRepo),
          row.getAs[Integer]("count"),
          java.util.UUID.randomUUID().toString

        )
    }

    val finalResult = sQLContext.createDataFrame(ir, new StructType(
      Array(
        StructField(username, StringType),
        StructField(projName, StringType),
        StructField(eventype, StringType),
        StructField(lang, StringType),
        StructField(userActivityRepo, StringType),
        StructField("count", IntegerType),
        StructField("sorter", StringType)
      )
    ))

    finalResult.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "edgeinfo", "keyspace" -> "git")).mode(SaveMode.Append).save()
  }
}
