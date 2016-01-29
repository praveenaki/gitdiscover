package com.virdis.cleanup

import org.apache.spark.sql.{SQLContext, DataFrame}
import Constants._

/**
  * Created by sandeep on 1/19/16.
  */
trait DataManipulator {

  def getDataByEventType(df: DataFrame, eventType: String): DataFrame = df.filter( df(EVENT_TYPE) === eventType )

  /*
      All S3 file => Dataframes
   */

  val allDFS: SQLContext => Seq[DataFrame] = sqlContext =>  {
    val df1 = sqlContext.read.json(S3_FILENAMES(0))
    val df2 =  sqlContext.read.json(S3_FILENAMES(1))
    val df3 =  sqlContext.read.json(S3_FILENAMES(2))
    val df4 =  sqlContext.read.json(S3_FILENAMES(3))
    val df5 =  sqlContext.read.json(S3_FILENAMES(4))
    val df6 =  sqlContext.read.json(S3_FILENAMES(5))
    val df7 =  sqlContext.read.json(S3_FILENAMES(6))
    val df8 =  sqlContext.read.json(S3_FILENAMES(7))
    val df9 =  sqlContext.read.json(S3_FILENAMES(8))
    val df10 =  sqlContext.read.json(S3_FILENAMES(9))
    val df11 =  sqlContext.read.json(S3_FILENAMES(10))
    val df12 =  sqlContext.read.json(S3_FILENAMES(11))

    IndexedSeq(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12)
  }


}
