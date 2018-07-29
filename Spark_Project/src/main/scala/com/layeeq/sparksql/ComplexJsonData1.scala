package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}


object ComplexJsonData1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonData1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val logger: Logger = Logger.getLogger("com.layeeq.sparksql.ComplexJsonData1")
    val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonData1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("ComplexJsonData1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
      val data = "C:\\work\\datasets\\JsonData\\world_bank.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("tab")
    val df1 = spark.sql("select * from  tab")
    df1.show()
    val ndf = spark.sql("select url, totalcommamt, totalamt, themecode, theme1.Name tname, theme1.Percent tpercent," +
      " theme_namecode from tab")
    ndf.show()
    val ndf1 = spark.sql("select url, totalcommamt, totalamt, themecode, theme1.Name tname, theme1.Percent tpercent," +
      " theme_namecode, t.code tcode , t.name tname from tab lateral view explode(theme_namecode) a as t")
    ndf1.show()
    val ndf2 = spark.sql("select url, totalcommamt, totalamt, themecode, theme1.Name tname, theme1.Percent tpercent, " +
      "t.code tcode , t.name tname from tab lateral view explode(theme_namecode) a as t")
     ndf2.show()
    ndf2.printSchema()
    val ndf3 = spark.sql("select _id.`$oid` idoid, supplementprojectflg, status, source, " +
      "sectorcode,sector4.Name secname, sector4.Percent secpercent,s.code, s.name,url, totalcommamt, " +
      "totalamt, themecode, theme1.Name tname, theme1.Percent tpercent, t.code tcode , t.name tname from " +
      "tab lateral view explode(theme_namecode) a as t lateral view explode(sector_namecode) a as s")

    ndf3.show()
    ndf3.printSchema()
    spark.sql("select _id.`$oid` idoid from tab").show()

    spark.stop()
  }
}