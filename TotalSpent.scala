package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/* Code to find the total amount spent by each customer
 * in "customer-orders.csv"
 */
object TotalSpent {
  
  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val dollarAmount = fields(2).toFloat
    
    (customerId, dollarAmount)
  }
  
  def main(args: Array[String]) {
    // Set Logger
    Logger.getLogger("org").setLevel(Level.ERROR)
  
    // Set Spark Context
    val sc = new SparkContext("local", "TotalSpent")
  
    // Extract CSV to an RDD
    val lines = sc.textFile("../customer-orders.csv")
    
    // Split the lines into separate fields (customerId, dollarAmount)
    // Map each line to key-value pairs of customerId and dollarAmount
    // Use reduceByKey to find amount spent for each customerId
    val customerAmounts = lines.map(parseLine).reduceByKey( (x,y) => x+y).sortByKey()
  
    // Collect the results
    val results = customerAmounts.collect()
  
    // Print the results
    for(result <- results) {
      val id = result._1
      val amount = result._2
      val formattedAmount = f"$$$amount%.2f"
      println(s"Customer $id spent $formattedAmount")
    }
  }
}