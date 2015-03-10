package tongji.dataspark.p4

/**
 * Problem4.scala
 */

import java.io.PrintWriter
import java.io.File

import org.apache.spark._
import org.apache.spark.SparkContext._

object Problem4 {

  def printUsage() {
    println("Usage:\nspark-submit\n--class tongji.dataspark.p4.Problem4\n--master <master-url>\nDataSpark_problem4.jar\ndatapath\nstopwordpath\nresultpath")
    System.exit(-1)
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      printUsage()
    }

    val conf = new SparkConf().setAppName("Problem4")
//                              .setMaster("local")
    val sc = new SparkContext(conf)

    val stopWordFile = sc.textFile(args(1))
    val txtFile = sc.textFile(args(0))

    val stopWordSet = stopWordFile.map(_.trim).collect
    val resultWord = txtFile.flatMap(_.split("[^-\\w+]|--")).map(_.toLowerCase)
                            .filter(word => !(word == "") && !stopWordSet.contains(word))
                            .map((_, 1)).reduceByKey(_ + _)
                            .map(_.swap).sortByKey(false).map(_.swap).cache

    val topHundredWord = resultWord.take(100)
    val pr = new PrintWriter(new File(args(2)))
    topHundredWord.foreach(word => pr.write(word._1 + "\n"))
    pr.close
  }

}
