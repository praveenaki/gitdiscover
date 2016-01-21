package com.virdis.cleanup

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by sandeep on 1/21/16.
  */
object GitDiscover {

  def main(args: Array[String]){

    val conf = new SparkConf(true).setAppName("GitDiscover")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    object gitMetrics extends Queries with DataManipulator

    val df = sqlContext.read.json("hdfs://52.34.172.205:9000/gitData/Jan15Days.json")

    gitMetrics.joinAcrossEventsByLangRepo(df)(sqlContext).take(10).foreach(println)

  }
}
