package com.layeeq.sparksql

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object GetOracleDBData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("GetOracleDBData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("GetOracleDBData").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("GetOracleDBData").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val url = "jdbc:oracle:thin://@oracledata.cnd0jjblcpqg.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val username = "ousername"
    val password = "opassword"

    //val tabs = Array("emp", "dept")
    //tabs.foreach { x =>
    val oprop = new java.util.Properties()
    oprop.setProperty("user", username)
    oprop.setProperty("password", password)
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
    //val df = spark.read.jdbc(url, "emp", oprop)
    //df.show()
    //val query = s"(select * from $x)"
    val df1 = spark.read.jdbc(url, "emp", oprop)
    df1.show()
   // df1.write.mode(SaveMode.Append).format("csv").option("header", "true").save(args(0))


    spark.stop()
  }
}
