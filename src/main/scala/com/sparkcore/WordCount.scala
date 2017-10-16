package com.sparkcore

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCountSpark").setMaster("local")
    val sc = new SparkContext(conf)

    if(args.length < 1) {
      println("Missing input file argument")
    }

    //Read a input text file
    val textFile = sc.textFile(args(0))

    //Split line into words
    val words = textFile.flatMap(line => line.split(" "))

    //Create a word tuple i.e. (word, 1)
    val wordTuple = words.map(word => (word, 1))

    //Reduce by word key
    val wordCount = wordTuple.reduceByKey((a,b) => (a+b))

    //Sort word counts
    val wordCountSorted = wordCount.sortByKey()

    //Print & word count
    wordCountSorted.foreach(println)
  }
}
