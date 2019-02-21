package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaxTemperatures {
  
  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temp)
  }
  
  def main(args: Array[String]) {
    // Set up the logger
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Set the Spark Context
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationId, entryType, temp)
    val parsedLines = lines.map(parseLine)
    
    // Filter out everything except max temperatures
    val maxTemps = parsedLines.filter(_._2 == "TMAX")
    
    // remove the entryType field such that (stationId, temp) remains
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    
    // Select only the maximum temperatures by station
    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y) )
    
    // collect the results
    val results = maxTempsByStation.collect()
    
    for( result <- results.sorted ) {
      val id = result._1
      val temp = result._2
      val formatedTemp = f"$temp%.2f F"
      
      println(s"$id maximum temperature is: $formatedTemp")
    }
    
    
  }
}
