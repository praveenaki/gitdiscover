package com.virdis.cleanup

import org.apache.spark.sql.SQLContext
import Constants._
/**
  * Created by sandeep on 2/1/16.
  */
trait EdgeAcrossProjects {

  self: CommonDataFunctions =>

  val userActivityRepo = "useractivityrepo"
  val username = "username"
  val projName = "projectname"
  def findEdge(sQLContext: SQLContext) = {
    val repostatsDF = sQLContext.read
                      .format("org.apache.spark.sql.cassandra")
                      .options(Map( "table" -> "repostats", "keyspace" -> "git" ))
                      .load().repartition(120) // no of cores

    val userActivityDF = sQLContext.read
                          .format("org.apache.spark.sql.cassandra")
                          .options(Map( "table" -> "repostats", "keyspace" -> "git" ))
                          .load().repartition(120) // no of cores

    val joinedRepo = repostatsDF.select(
                        repostatsDF(projName),
                        repostatsDF("eventcommitter").as(username),
                        repostatsDF("eventtype"),
                        repostatsDF("language")
                      ).join(
                        userActivityDF.select(
                          userActivityDF("projectname").as(userActivityRepo),
                          userActivityDF(username)
                        ), username
                      ).persist()

    val filterdJoin = joinedRepo.filter( joinedRepo(projName) !== joinedRepo(userActivityRepo) ).persist()



  }
}
