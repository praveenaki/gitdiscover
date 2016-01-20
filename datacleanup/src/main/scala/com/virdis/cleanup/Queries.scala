package com.virdis.cleanup

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
  * Created by sandeep on 1/20/16.
  */
trait Queries {
  self: DataManipulator =>

  def countEventsByRepo(eventType: String, df: DataFrame) = {
    getDataByEventType(df, eventType).groupBy("repo.name").agg(count("repo.name").alias("total")).sort(desc("total"))
  }



}
