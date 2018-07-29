package com.layeeq.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object KafkaConsumer {
  case class Uscc(first_name:String, last_name:String, company_name:String, address:String, city:String, county:String, state:String,zip:String, phone1:String, phone2:String, email:String, web:String)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KafkaConsumer").set("spark.driver.allowMultipleContexts", "true").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(10))
    import scala.util.Try

    val topics = "layeeq5"
    val kafkabroker = "localhost:9092"
    val topicSet = topics.split(";").toSet
    val kakfkaParams = Map[String, String]("metadata.broker.list" -> kafkabroker)

    import org.apache.spark.streaming.kafka._
    //import kafka.serializer.DefaultDecoder


    //import kafka.serializer.StringDecoder
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kakfkaParams, topicSet)

    //    import kafka.serializer.StringDecoder
    //val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kakfkaParams,topicSet)

    lines.foreachRDD { x =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val df = x.map(x => x._2).map(x => x.split(";")).map(x => Uscc(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))).toDF()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab")
      //res.show()
      val url = "jdbc:oracle:thin://@oracledb.cnd0jjblcpqg.ap-south-1.rds.amazonaws.com:1521/ORCL"

      //val tabs = Array("emp", "dept")
      //tabs.foreach { x =>
      val oprop = new java.util.Properties()
      oprop.setProperty("user", "ousername")
      oprop.setProperty("password", "opassword")
      oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
      res.write.mode(SaveMode.Append).jdbc(url,"usadata",oprop)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()

  }
}



