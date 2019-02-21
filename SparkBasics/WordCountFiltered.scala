package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/* Count the unique words in a document, ignoring common words (ie. the, a, is, ...) */
object WordCountFiltered {
  
  def main(args: Array[String]) {
    
    // Set the logger
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Set the Spark Context
    val sc = new SparkContext("local", "WordCountFiltered")
    
    // Set the list of words to ignore
    val commonWords = sc.textFile("../ignore.txt")
    
    // Split by comma
    val ignore = commonWords.flatMap(x => x.split(",")).collect().toList
    
    // Read the lines in
    val lines = sc.textFile("../book.txt")
    
    // Split into individual words
    val words = lines.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Filter out common words
    val interestingWords = lowercaseWords.filter(!ignore.contains(_))
    
    // Count the interesting words
    val wordCount = interestingWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y)
    
    // Sort by count
    val sortedCount = wordCount.map(x => (x._2, x._1)).sortByKey()
    
    // Collect the results
    val results = sortedCount.collect()
    
    // Print the results
    for(result <- results) {
      val count = result._1
      val word = result._2
      
      println(s"$word: $count")
    }
  }
  
}
