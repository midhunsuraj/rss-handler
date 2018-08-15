package com.probi.spark.rss


import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.probi.spark.rss.RSSInputDStream

import scala.io.Source

object RssFeedProcessor {

  def main(args: Array[String]): Unit = {

    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    val urlCSV = Source.fromFile("feedurls.csv").getLines().mkString(",")
    val urls = urlCSV.split(",")

    val stream = new RSSInputDStream(urls, Map[String, String](), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)



    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._

      rdd.toDS().show(20,false)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
