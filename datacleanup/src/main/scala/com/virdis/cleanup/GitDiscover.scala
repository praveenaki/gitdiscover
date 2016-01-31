package com.virdis.cleanup

import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.typesafe.config.ConfigFactory

/**
  * Created by sandeep on 1/21/16.
  */
object GitDiscover {


  def main(args: Array[String]){

    val conf = new SparkConf(true)
                .set("spark.executor.memory", "14g")
                .set("spark.driver.memory", "14g")
                .set("sandeep-cassandra-cluster:git/spark.cassandra.input.split.size_in_mb","128")
                .set("spark.cassandra.connection.host","172.31.2.69")
                .setAppName("GitDiscover")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val config = ConfigFactory.load()


    object gitMetrics extends TopProjectQuery with RepoTimeSeries with CommonDataFunctions


    try {
      if (!config.getBoolean("job.name.topproject.status")) {
        println("========== TOPPROJECTS JOB ============")
        gitMetrics.topProjects(sqlContext)
      }
      if (!config.getBoolean("job.name.repotimeseries.status")) {
        println("=========== REPOTIMESERIES JOB ============")
        gitMetrics.repoTimeSeries(sqlContext)
      }

      if (!config.getBoolean("job.name.userstats")) {

      }

    } catch {
      case e: Exception =>
        println("==== Exception ===="+e.getCause)
        println("=== Localized Message ==="+e.getLocalizedMessage)
        println("=== Stack Trace ===="+e.printStackTrace())
        println("=== Throw ====")
        throw e
    }
  }
}
