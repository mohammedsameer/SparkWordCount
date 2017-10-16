package com.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object WordCount {

  //Run
  //Terminal1: Run WordCount
  //Terminal2: nc -lk 9999
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCountSparkStream").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    //Split line into words
    val words = lines.flatMap(line => line.split(" "))

    //Create a word tuple i.e. (word, 1)
    val wordTuple = words.map(word => (word, 1))

    //Reduce by word key
    val wordCount = wordTuple.reduceByKey((a,b) => (a+b))

    wordCount.print()

    //Start the stream
    ssc.start()
    //Wait for process termination
    ssc.awaitTermination()
  }
}
