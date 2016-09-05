package com.oopsdata.lake

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
 
object SparkWordCount {
  def main(args: Array[String]) { 
   
//    // Create a dense vector (1.0, 0.0, 3.0).
//    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
//    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
//    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
//    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
//    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
//    
//    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
//    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
//    
//    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
//    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
//    println(sm)
    args.foreach(println)
    val inputfile = args(2)
    val outputfile = args(3)
    val conf: SparkConf = new SparkConf()
                .setAppName("spark-learning")
                .setMaster("local")
                .set("spark.executor.memory", "1g")
    val sc: SparkContext = new SparkContext(conf)
    val lines = sc.textFile(inputfile)
    //lines.foreach(println)
    //lines.filter(_.contains("show")).foreach(println)
    
    val words = lines.flatMap(_.split(" "))
    val counts = words.map((_, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(outputfile)
  }
}








