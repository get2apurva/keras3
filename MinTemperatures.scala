package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  case class Temperature(no:String,stationID : String,entryType:String,temperature:Float)
  
//  def parseLine(line:String)= {
//    val fields = line.split(",")
//    val stationID = fields(0)
//    val entryType = fields(2)
//    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
//    (stationID, entryType, temperature)
//  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    //val sc = new SparkContext("local[*]", "MinTemperatures")
    

import org.apache.spark.sql._

    
val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:/// C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
            .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 6)
       
    

    
    // Read each line of input data
    val lines = spark.sparkContext.textFile("../1800.csv")
     //val people = lines.map(x =>x.split(",")).map(fields =>Person(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt) )
     //case class Temperature(stationID : String,entryType:String,temperature:Int)
    
    

    
    println(lines.count)
    
    val df=lines.map(x=> x.split(",")).map(fields => Temperature(fields(0),fields(1),fields(2),fields(3).toFloat) )
    
//    df.take(10).foreach(println)
//    
//     val rdd_1 =df.collect()
//     
//       for (result <- rdd_1) {
//       val station = result._1
//       val temp = rdd_1(3)
//       
//       println(s"$station")
//       
//      // println(s"$station minimum temperature: $temp") 
//    }
    
    

    
    
    println(df.count)
    import spark.implicits._
    val schemaTemperature =df.toDS
  
    
    schemaTemperature.filter($"entryType"==="TMIN")
    schemaTemperature.show(5)
    
    
    
//    schemaTemperature.createTempView("Temperature_View")
//   val results = spark.sql("""select stationID,temperature from Temperature_View where entryType ="TMIN" """ )
//    results.show(5)
//   results.createTempView("final_view")
//   
//   spark.sql(""" select stationID,min(temperature) from final_view group by stationID""").show(10)


    
    
//    // Convert to (stationID, entryType, temperature) tuples
//    val parsedLines = lines.map(parseLine)
//    
//    // Filter out all but TMIN entries
//    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
//    
//    // Convert to (stationID, temperature)
//    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
//    
//    // Reduce by stationID retaining the minimum temperature found
//    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))
//    
//    // Collect, format, and print the results
//    val results = minTempsByStation.collect()
//    
//    for (result <- results.sorted) {
//       val station = result._1
//       val temp = result._2
//       val formattedTemp = f"$temp%.2f F"
//       println(s"$station minimum temperature: $formattedTemp") 
//    }
//      
  }
}