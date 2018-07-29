package com.layeeq.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Example5 {

  case class aslcc(name:String,age:Int,city:String)
  case class nepcc(name:String,age:Int,city:String)
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val logger: Logger = Logger.getLogger("com.layeeq.sparkcore.Example5")
    //val spark = SparkSession.builder.master("local[*]").appName("Example5").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Example5").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Example5").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val num1= Array(1,2,3,4,5,6,7,8,9,10)
    val rdd1 = sc.parallelize(num1)
    val num2 = Array(2,3,4,5,6)
    val rdd2= sc.parallelize(num2)
    val union = rdd1.union(rdd2).distinct()
    union.collect().foreach(println)
    val intersect1 =rdd1.intersection(rdd2)
    intersect1.collect().foreach(println)
    /*--------------------------------------------*/
    val data = "C:\\work\\datasets\\dataset.txt"
    val drdd = sc.textFile(data)
    val reduceBK = drdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y) => x+y).sortBy(x => x._1, false)
    val groupBK = drdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).groupByKey().map(x =>(x._1, x._2.sum)).sortBy(x => x._1, false)
    reduceBK.take(20 ).foreach(println)
    groupBK.take(20 ).foreach(println)
    /*-------------------------------------------------------*/
    val asl = "C:\\work\\datasets\\asl.csv"
    val rasl = sc.textFile(asl)
    //val head1 = rasl.first()
    //val ardd = rasl.filter(x =>x != head1)

    val nep = "C:\\work\\datasets\\nep.csv"
    val rnep= sc.textFile(nep)
    val head2 = rnep.first()
    val nrdd = rnep.filter(x =>x != head2)
    val aslrdd = rasl.map(x => x.split(",")).map(x=>(x(0), x(1),x(2)))//name,age,city
    val neprdd = nrdd.map(x=>x.split(",")).map(x=>(x(0), x(1),x(2)))//name,email,phone
    val akey= aslrdd.keyBy(x=>x._1)
    val nkey= neprdd.keyBy(x=>x._1)

    val join =akey.join(nkey)
    join.collect().foreach(println)
    join.map(x=>x._1).collect().foreach(println)
    join.map(x=>(x._1,x._2)).collect().foreach(println)
    join.map(x=>(x._1, x._2._1)).collect().foreach(println)
    join.map(x=>(x._1, x._2._1._3)).collect().foreach(println)
    join.map(x=>(x._1, x._2._1._3,x._2._2._2)).collect().foreach(println)
    /*-------------------------------------------------------*/
    spark.stop()
  }
}