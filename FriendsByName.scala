package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
  
  def parseLine(line: String) = {
    //Split by commas
    val fields = line.split(",")
    
    val age = fields(2).toInt
    val name = fields(1).toString
    
    (name, age)
  }
  
  def main(args: Array[String]) {
    
    // Set logger
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create spark context
    val sc = new SparkContext("local[*]","FriendsByName")
    
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../fakefriends.csv")
    
    // Use parseLine function to create RDD with (name, age)
    val rdd = lines.map(parseLine)
    
    // map reduce such that totalsByName is (Name, (Age, Count))
    val totalsByName = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    
    // Calculate average by dividing Age/Count
    val averageAgeByName = totalsByName.mapValues(x => x._1/x._2)
    
    val results = averageAgeByName.collect()
    
    results.sorted.foreach(println)
    
  }
  
}