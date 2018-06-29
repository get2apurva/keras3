package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/** Find the movies with the most ratings. */
object PopularMovies {
 
  case class Movie(user_id:String,movie_id:Int,rating:String, time:String)
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
   val  spark =SparkSession
    .builder
    .appName("popularMovies")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:/// C:/temp")
    .getOrCreate()
     import spark.implicits._
    
     // Create a SparkContext using every core of the local machine
   // val sc = new SparkContext("local[*]", "PopularMovies")   
    
    // Read in each rating line
    //val lines = sc.textFile("../ml-100k/u.data")
    
//  val lines = spark.read.csv("../ml-100k/u.data")
//    .toDF("user_id","movie_id","rating","time")
//    .as[Movie]
    
    

     // case class Movie(user_id:String,movie_id:Int,rating:String, time:String)
    val lines2 =spark.sparkContext.textFile("../ml-100k/u.data")

   val rdd= lines2.map(x=>x.split("\t")).map(fields => Movie(fields(0),fields(1).toInt,fields(2),fields(3)   ) )
    
    val dataframe =rdd.toDS()
    
    
    //dataframe.show(5)
    
    dataframe.createTempView("View")
    
    
   val grp= dataframe.groupBy("movie_id").count().orderBy(desc("count"))
   
   grp.show(10)
    
    spark.sql("""select movie_id ,count(movie_id) as cnt from View
      group by movie_id order by cnt  """).show(5)
    
    
//    lines.createOrReplaceTempView("data")
//    lines.show(5)
    
//    // Map to (movieID, 1) tuples
//    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
//    
//    // Count up all the 1's for each movie
//    val movieCounts = movies.reduceByKey( (x, y) => x + y )
//    
//    // Flip (movieID, count) to (count, movieID)
//    val flipped = movieCounts.map( x => (x._2, x._1) )
//    
//    // Sort
//    val sortedMovies = flipped.sortByKey()
//    
//    // Collect and print results
//    val results = sortedMovies.collect()
//    
//    results.foreach(println)
  }
  
}

