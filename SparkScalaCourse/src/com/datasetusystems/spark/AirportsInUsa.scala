package com.datasetusystems.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AirportsInUsa {
  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val airports = sc.textFile("../airports.text")
    val airportsInUsa = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3)=="\"United States\"")
    
    val airportsNameAndCityNames = airportsInUsa.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + "," + splits(2)
    })
    
    //airportsNameAndCityNames.map(println)
    airportsNameAndCityNames.foreach(println)
  }
}