package com.virdis.cleanup

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
  * Created by sandeep on 1/21/16.
  */
object GitDiscover {

  def main(args: Array[String]){

    val conf = new SparkConf(true)
                .set("spark.cassandra.connection.host","52.88.244.205")
                .setAppName("GitDiscover")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    object gitMetrics extends Queries with DataManipulator

    val df = sqlContext.read.json("hdfs://52.34.172.205:9000/gitData/Jan15Days.json")

    val topPrjs = gitMetrics.joinAcrossEventsByLangRepo(df)(sqlContext)

    topPrjs.map(r => r).saveToCassandra("git_project","repos", SomeColumns("name" as "_2","language" as "_1","eventsTotal" as "_3"))

  }
}
