package com.layeeq.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object KafkaProducer {
  def main(args: Array[String]) {
    val topic: String = "big1"

    val spark = SparkSession.builder.master("local[2]").appName("structurestreaming").
      config("spark.sql.streaming.checkpointLocation","file:///home/hadoop/work/datasets/checkpoint").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //val data = args(0)
    val data = "C:\\work\\datasets\\kafkalogs\\us-500.csv"
    val my = sc.textFile(data)
    val head = my.first()
    val finalrdd = my.filter(x=>x!=head)//.map(x=>x.split(";"))

    finalrdd.foreachPartition(rdd => {
      import java.util._
      val props = new java.util.Properties()
      props.put("metadata.broker.list", "localhost:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")

      import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config) // key value

      rdd.foreach(x => {
        println(x) //not recomanded in productions
        producer.send(new KeyedMessage[String, String](topic.toString(), x.toString)) //not recomanded in productions
        Thread.sleep(1000)
      })

    })
  }
}
