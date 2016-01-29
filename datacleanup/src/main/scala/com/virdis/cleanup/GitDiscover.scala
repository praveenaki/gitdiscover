package com.virdis.cleanup

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
  * Created by sandeep on 1/21/16.
  */
object GitDiscover {


  def main(args: Array[String]){

    val conf = new SparkConf(true)
                .set("sandeep-cassandra-cluster:git/spark.cassandra.input.split.size_in_mb","64")
                .set("spark.cassandra.connection.host","172.31.2.69")
                .setAppName("GitDiscover")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    

    object gitMetrics extends TopProjectQuery with DataManipulator




    //topPrjs.show()

    try {
      val topPrjs = gitMetrics.topProjects(sqlContext)

      topPrjs.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "toprepos", "keyspace" -> "git")).mode(SaveMode.Append).save()

    } catch {
      case e: Exception =>
        println("==== Exception ===="+e.getCause)
        println("=== Localized Message ==="+e.getLocalizedMessage)
        println("=== Stack Trace ===="+e.printStackTrace())
        println("=== Throw ====")
    }
  }
}
