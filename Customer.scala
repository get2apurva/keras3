package com.sundogsoftware.spark2
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.broadcast

object Customer  {
  
  case class Customer(id:String,product_id:String,amt:Float)
  case class Customer2(id:String,product_id:String,amt:Float)
  def main(args :Array[String]){
    import StructType, StructField, IntegerType, StringType, BooleanType
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder.appName("countbyspent")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:/// C:/temp")
    .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 6)
   import spark.implicits._
    
    //val schema_ds =spark.read.csv("../customer-orders.csv").as[Customer]
    
    

    
    
        val cities = spark.read
  .option("header", "false")
  .csv("../customer-orders.csv")
  .toDF("id", "product_id", "amt")
  .withColumn("amt", col("amt").cast(FloatType))
  .as[Customer2]
    
    
    cities.show(5)
    
    val lines =spark.sparkContext.textFile("../customer-orders.csv")
    
    val fields=lines.map(x => x.split(",")).map(fields => Customer(fields(0),fields(1),fields(2).toFloat))
    
    
    val data =fields.toDS
    
    data.createOrReplaceTempView("orders_view")
    
    
   val cnt= data.groupBy("id").agg(sum("amt")).orderBy(desc("sum(amt)"))
   val windowoverCategory =  Window.orderBy((desc("sum(amt)")))
    val order =cnt.withColumn("dense_rank" , dense_rank().over(windowoverCategory))
    
    order.filter($"dense_rank" ===1).show(10)
    //cnt.withColumn("rank", dense_rank().over(Window.orderBy(desc("sum(amt)")))).show(5)
    

    
   //data.groupBy("id").agg(sum("amt")).alias("total_amt_spent").show()
    //data.select("id", "amt").show()
    
    val total =spark.sql(""" select id,sum(amt) as total_amt_spent from orders_view
     group by id order by total_amt_spent desc """)
    
    total.createOrReplaceTempView("total_view")
    
    spark.sql(""" select * from (select  * ,dense_rank() over ( order by total_amt_spent desc ) as ranking from  total_view ) as a where a.ranking=1 """).show(20)
    
    spark.catalog.listTables.show()
    spark.table("total_view").show()
    
    
    spark.catalog.createExternalTable("sales_external", "com.databricks.spark.csv", Map(
 "path" -> "../customer-orders.csv",
 "header" -> "true"))
spark.table("sales_external").show()
    
    
  }
  
}