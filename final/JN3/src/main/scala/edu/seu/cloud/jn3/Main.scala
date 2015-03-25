package edu.seu.cloud.jn3

import java.io.PrintWriter

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}


object Main extends Serializable {

  val conf = new SparkConf().setAppName("JN3")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    val minPartitions = 192
    val confidence = 0.85

    val transactions = sc.textFile(args(0), minPartitions)
    var filteredTransactions = transactions.map(_.split(" ").drop(1))
    val transactionCount = transactions.count()
    val minSupport = math.ceil(transactionCount * confidence).toLong

    val oneItemHS = HashSet[String]()
    var lastItemSet = sc.parallelize(Array((1, HashSet[String]())))

    (1 to 8).foreach(findFrequentItemSet)

    def findFrequentItemSet(iterationNum: Int) {

//      val out = new PrintWriter(args(1) + "/result-" + iterationNum)

      val outputPath = args(1) + "/result-" + iterationNum

      if (iterationNum == 1) {
        val oneItemSet = transactions.flatMap(_.split(" ").drop(1)).map((_, 1)).reduceByKey(_ + _).filter(_._2 >= minSupport).cache
        lastItemSet = oneItemSet.map(item => {
//          lastItemHS += HashSet(item._1)
          (1, HashSet(item._1))
        }).cache

        oneItemSet.collect.foreach(item => {
          oneItemHS.add(item._1)
        })

        oneItemSet.map(item => {
          "%s:%s".format(item._1, item._2 * 1.0 / transactionCount)
        }).saveAsTextFile(outputPath)

        filteredTransactions = filteredTransactions.map(_.filter(oneItemHS.contains)).cache
      } else {
        val lastItemHS = HashSet(HashSet[String]())
        lastItemSet.collect.foreach(lastItemHS += _._2)
        val candidates = (lastItemSet join lastItemSet).map(item => item._2._1 ++ item._2._2).filter(_.size == iterationNum).distinct.filter(candidate => {
          val tempHS = candidate.clone
          var pruneFlag = true

          tempHS.foreach(item => {
            tempHS.remove(item)

            if (!lastItemHS.contains(tempHS)) {
              pruneFlag = false
            }

            tempHS.add(item)
          })

          pruneFlag
        }).collect

        val itemSet = filteredTransactions.flatMap(transaction => {

          val newItemSet = ArrayBuffer[String]()

          candidates.foreach(candidate => {
            var containAll = true
            candidate.foreach(item => {
              if (!transaction.contains(item)) {
                containAll = false
              }
            })

            if (containAll == true) {
              newItemSet += candidate.toString
            }

          })

          newItemSet
        }).map((_, 1)).reduceByKey(_ + _).filter(_._2 >= minSupport).cache

        lastItemSet = itemSet.map(item => {
          val sh = HashSet[String]()
          item._1.substring(4, item._1.length - 1).trim.split(", ").foreach(sh.add)
          (1, sh)
        })

        itemSet.map(item => {
          "%s:%s".format(item._1.substring(4, item._1.length - 1).trim.split(", ").mkString(","), item._2 * 1.0 / transactionCount)
        }).saveAsTextFile(outputPath)
      }
    }

  }

}
