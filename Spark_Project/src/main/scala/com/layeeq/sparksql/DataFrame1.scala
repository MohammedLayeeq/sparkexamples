package com.layeeq.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataFrame1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("DataFrame1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("DataFrame1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("DataFrame1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\datasets\\us-500.csv"
    val usrdd = sc.textFile(data)
    val head = usrdd.first()
    //val pro1 = usrdd.filter(x=>x !=head).map(x=>x.split(";")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))).toDF()
    //pro1.show()
    //pro1.printSchema()
    //pro1.show()
    //"first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web" --> must me in sequence order as per structure

    val pro2 = usrdd.filter(x=>x !=head).map(x=>x.split(";")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))).toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
    pro2.show()
    pro2.createOrReplaceTempView("ustable")
    val usselect1 = spark.sql("select first_name, last_name,state,city from ustable where state='LA'")
    usselect1.show()
    val usselect2 = spark.sql("select  state, count(*) cnt from ustable  GROUP BY state ORDER BY cnt")
    usselect2.show()



    spark.stop()
  }
}