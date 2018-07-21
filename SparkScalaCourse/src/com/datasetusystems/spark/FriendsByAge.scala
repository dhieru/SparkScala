package com.datasetusystems.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._
import breeze.linalg.mapValues

object FriendsByAge {
  
  // A function that splits a line of input into (age, numFriends) tuples 
  def parseLine(line : String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and number of friends field
   // fields.foreach(println)
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    
    // create a tuple that is our result
    (age,numFriends)
  }
  
  def main(args : Array[String]) = {
    // Set the log level to only print the error
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a Spark conf object
    val conf = new SparkConf().setAppName("FriendsByAge").setMaster("local[*]")
    
    // Create a Spark Context object
    val sc = new SparkContext(conf)
    
    // Load each line of the data source in an RDD
    val lines = sc.textFile("../fakefriends.csv")
    val rdd = lines.map(parseLine)
    
    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((a,b)=> (a._1+ b._1, a._2+b._2))
    val avgByAge = totalsByAge.mapValues(x => x._1/x._2)
    val results = avgByAge.collect()
    
    results.sorted.foreach(println)
  }
}