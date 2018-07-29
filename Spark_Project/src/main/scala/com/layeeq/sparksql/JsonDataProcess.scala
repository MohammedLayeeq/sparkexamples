package com.layeeq.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object JsonDataProcess {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val logger: Logger = Logger.getLogger("com.layeeq.sparksql.JsonDataProce")
    //val spark = SparkSession.builder.master("local[*]").appName("JsonDataProcess").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("JsonDataProcess").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("JsonDataProcess").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    val data = "C:\\work\\datasets\\JsonData\\zips.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("tab")
    val ndf1 = spark.sql("select _id id , city , loc[0] langitude , loc[1] latitude , pop, state from tab")
    val ndf2 = spark.sql("select _id id , city , abs(loc[0]) langitude , loc[1] latitude , pop, state from tab")


   /* val url = "jdbc:oracle:thin://@oracledb.cnd0jjblcpqg.ap-south-1.rds.amazonaws.com:1521/ORCL"

    //val tabs = Array("emp", "dept")
    //tabs.foreach { x =>
    val oprop = new java.util.Properties()
    oprop.setProperty("user", "ousername")
    oprop.setProperty("password", "opassword")
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
    ndf2.write.mode(SaveMode.Append).jdbc(url,"usadata",oprop)
    //ndf2.write.saveAsTable(table)*/


    ndf1.show()
    //ndf2.show()
    spark.stop()
  }
}