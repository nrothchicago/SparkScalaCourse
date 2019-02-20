package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaxPrecip {
  
  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val precip = fields(3).toFloat / 2.54f
    (stationId, entryType, precip)
  }
  
  def main(args: Array[String]) {
    // Set up the logger
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Set the Spark Context
    val sc = new SparkContext("local[*]","MaxPrecip")
    
    // Read in the CSV file
    val lines = sc.textFile("../1800.csv")

    // Parse the lines
    val parsedLines = lines.map(parseLine)
    
    // Select only values related to precipitation
    val precipitation = parsedLines.filter(_._2 == "PRCP")
    
    // Remove the entryType such that (stationId, precip)
    val stationPrecip = precipitation.map(x => (x._1,x._3))
    
    // Reduce to find maximum precipitation
    val maxPrecip = stationPrecip.reduceByKey((x,y) => max(x,y))

    // Collect the results
    val results = maxPrecip.collect()
    
    // Print out the formatted results
    for(result <- results) {
      val stationId = result._1
      val precip = result._2
      val formattedPrecip = f"$precip%.2f inches"
      
      println(s"$stationId has max precipitation of $formattedPrecip")
    }
    
    
  }
  
}