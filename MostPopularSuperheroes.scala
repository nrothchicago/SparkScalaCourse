package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the top 10 superheroes */
object MostPopularSuperheroes {
  
  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    val elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None
    }
  }
  
  /** the main method */
  def main(args: Array[String]) {
    
    // Set Logger
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Set Spark Context
    val sc = new SparkContext("local","MostPopularSuperheroes")
    
    // Create Rdd with ( heroID, heroName)
    val names = sc.textFile("../Marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // Get super hero co-apperances
    val lines = sc.textFile("../Marvel-graph.txt")
    
    // Convert to (heroId, totalConnections) RDD
    val pairings = lines.map(countCoOccurences)
    
    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    
    // Flip to (totalConnections, heroId)
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
    
    // Sort by the total number of connections
    val mostPopularNames = flipped.sortByKey(false)
    
    val top10 = mostPopularNames.take(10)
    
    for(hero <- top10) {
      val heroName = namesRdd.lookup(hero._2)(0)
      val coOccurences = hero._1
      println(s"$heroName had $coOccurences co-occurences")
    }
    
    

    
    
  }
}