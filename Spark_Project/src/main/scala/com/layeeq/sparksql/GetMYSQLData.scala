package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object GetMYSQLData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("GetMYSQLData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("GetMYSQLData").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("GetMYSQLData").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val url = "jdbc:mysql://mysqldb.cnd0jjblcpqg.ap-south-1.rds.amazonaws.com:3306/mysqldatabase"
    val username= "musername"
    val password = "mpassword"
    val mprop= new java.util.Properties()
    mprop.setProperty("user",username)
    mprop.setProperty("password", password)
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
    val df = spark.read.jdbc(url,"emp",mprop)
    df.show()


    spark.stop()
  }
}