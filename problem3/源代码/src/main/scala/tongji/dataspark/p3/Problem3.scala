package tongji.dataspark.p3

import java.io.PrintWriter

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Problem3.scala
 */
object Problem3 {

  def printUsage() {
    println("Usage:\nspark-submit\n--class tongji.dataspark.p3.Problem3\n--master <master-url>\nDataSpark_problem3.jar inputpath outputpath")
    System.exit(-1)
  }

  val conf = new SparkConf().setAppName("Problem3")
//                            .setMaster("local")
//                            .set("spark.local.dir", "data/tmp")
//                            .set("spark.executor.memory", "10g")

  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    if (args.length < 2) {
      printUsage
    }

    val minPartitions = 64
    val minSupportRate = 0.85

    val inputFile = sc.textFile(args(0), minPartitions)

    val transactions = inputFile.map(_.split("\\s+").drop(1))
    val transactionCount = transactions.count
    val minSupport = math.ceil(transactionCount * minSupportRate).toInt

    // sorted frequent one-item set
    val oneItemSet = inputFile.flatMap(_.split("\\s+").drop(1)).map((_, 1)).reduceByKey(_ + _)
                          .filter(_._2 >= minSupport)
                          .map(_.swap).sortByKey(false).map(_.swap).collect

    val groupNum = oneItemSet.length

    // (item, frequency) -> (item, id)
    val oneItemMap = new HashMap[String, Int]()
    var id = 0
    oneItemSet.foreach(pair => {
                        oneItemMap.put(pair._1, id)
                        id = id + 1
                      })

    var maxNumPerGroup = oneItemSet.length / groupNum
    if (oneItemSet.length % groupNum != 0) {
      maxNumPerGroup += 1
    }

    val finalPatterns = new ArrayBuffer[(String, Int)]()

    println("Map Start...")
    // group, build tree recursively and get pattern
    val frePatternsMap = transactions.flatMap(SparkFPGrowthMapper().map(_, oneItemMap, maxNumPerGroup)).groupByKey.cache
    println("Map End...")

    println("Reduce Start...")
    val frePatternsReduce = frePatternsMap.flatMap(SparkFPGrowthReducer().reduce(_, minSupport)).groupByKey.collect
    println("Reduce End...")

    for (pattern <- frePatternsReduce) {
      val length = pattern._1.split(",").length
      val resultStr = "%s:%.2f".format(pattern._1, pattern._2.max * 1.0 / transactionCount)
      finalPatterns += resultStr -> length
    }

    oneItemSet.foreach { pair =>
      val resultStr = "%s:%.2f".format(pair._1, pair._2 * 1.0 / transactionCount)
      finalPatterns += resultStr -> 1
    }

    val out1 = new PrintWriter(args(1) + "/result-1.txt")
    val out2 = new PrintWriter(args(1) + "/result-2.txt")
    val out3 = new PrintWriter(args(1) + "/result-3.txt")
    val out4 = new PrintWriter(args(1) + "/result-4.txt")
    val out5 = new PrintWriter(args(1) + "/result-5.txt")
    val out6 = new PrintWriter(args(1) + "/result-6.txt")
    val out7 = new PrintWriter(args(1) + "/result-7.txt")
    val out8 = new PrintWriter(args(1) + "/result-8.txt")

    finalPatterns.foreach(pattern =>
      pattern._2 match {
        case 1 => out1.println(pattern._1)
        case 2 => out2.println(pattern._1)
        case 3 => out3.println(pattern._1)
        case 4 => out4.println(pattern._1)
        case 5 => out5.println(pattern._1)
        case 6 => out6.println(pattern._1)
        case 7 => out7.println(pattern._1)
        case 8 => out8.println(pattern._1)
      })

    out1.close
    out2.close
    out3.close
    out4.close
    out5.close
    out6.close
    out7.close
    out8.close

    sc.stop()
  }

  // Map
  object SparkFPGrowthMapper {
    def apply(): SparkFPGrowthMapper = {
      new SparkFPGrowthMapper()
    }
  }

  class SparkFPGrowthMapper {
    def map(transaction: Array[String], oneItemMap: HashMap[String, Int], maxNumPerGroup: Int): ArrayBuffer[(Int, ArrayBuffer[String])] = {
      // (groupID, items)
      val groupItems = new ArrayBuffer[(Int, ArrayBuffer[String])]()

      var filteredTrans = new ArrayBuffer[Int]()
      transaction.filter(oneItemMap.keySet.contains(_)).foreach(filteredTrans += oneItemMap(_))
      filteredTrans = filteredTrans.sortWith(_ > _)

      // group
      val groups = new ArrayBuffer[Int]()
      for (i <- 0 until filteredTrans.length) {
        val groupID = filteredTrans(i) / maxNumPerGroup
        if (!groups.contains(groupID)) {
          groups += groupID
          groupItems += groupID -> filteredTrans.slice(i, filteredTrans.length).map(item => oneItemMap.map(_.swap).getOrElse(item, null))
        }
      }

      groupItems
    }
  }

  // Reduce
  object SparkFPGrowthReducer {
    def apply(): SparkFPGrowthReducer = {
      new SparkFPGrowthReducer()
    }
  }

  class SparkFPGrowthReducer {
    def reduce(groupTrans: (Int, Iterable[ArrayBuffer[String]]), minSupport: Int): ArrayBuffer[(String, Int)] = {
      val transactions = groupTrans._2
      val tree = new FPTree(new ArrayBuffer[(String, Int)]())
      tree.FPGrowthRun(transactions, new ArrayBuffer[String], minSupport)
      tree.patterns
    }
  }

  class FPTree (val patterns: ArrayBuffer[(String, Int)]) {

    def FPGrowthRun(transactions: Iterable[ArrayBuffer[String]], postPatterns: ArrayBuffer[String], minSupport: Int) {
      val headers = buildHeaderArray(transactions, minSupport)
      val root = buildFPTree(transactions, headers)

      if (root.children.isEmpty) {
        return
      }

      if (!postPatterns.isEmpty) {
        for (header <- headers) {
          val tempPattern = new ArrayBuffer[String]()
          tempPattern += header.name
          for (pattern <- postPatterns) {
            tempPattern += pattern
          }
          val pattern = (tempPattern.sortWith(_ < _).mkString(",").toString, header.count)
          if (!patterns.contains(pattern)) {
            patterns += pattern
          }
        }
      }

      headers.map(header => {
        val newPostPatterns = new ArrayBuffer[String]()
        newPostPatterns += header.name
        if (!postPatterns.isEmpty) {
          newPostPatterns ++= postPatterns
        }

        val newTransactions = new ArrayBuffer[ArrayBuffer[String]]()
        var tempNode = header.nextSibling
        while (tempNode != null) {
          var count = tempNode.count
          val preNodes = new ArrayBuffer[String]()
          var parent = tempNode.parent

          while (parent.name != null) {
            preNodes += parent.name
            parent = parent.parent
          }

          while (count > 0) {
            newTransactions += preNodes
            count -= 1
          }
          tempNode = tempNode.nextSibling
        }

        FPGrowthRun(newTransactions, newPostPatterns, minSupport)
      })

    }

    def buildHeaderArray(transactions: Iterable[ArrayBuffer[String]], minSupport:Int): ArrayBuffer[TreeNode] = {
      if (transactions.isEmpty) {
        return null
      }

      val headers = new ArrayBuffer[TreeNode]()

      val nodeMap = new HashMap[String, TreeNode]()
      for (transaction <- transactions) {
        for (item <- transaction) {
          if (nodeMap.contains(item)) {
            nodeMap(item).count += 1
          } else {
            val node = new TreeNode(name = item, count = 1)
            nodeMap += item -> node
          }
        }
      }

      nodeMap.values.filter(_.count >= minSupport).toArray.sortWith(_.count > _.count).foreach(headers += _)

      headers
    }

    def buildFPTree(transactions: Iterable[ArrayBuffer[String]], headers: ArrayBuffer[TreeNode]): TreeNode = {

      def sortTransaction(transaction: ArrayBuffer[String]): ArrayBuffer[String] = {
        val sortedTransaction = new ArrayBuffer[String]()
        val itemMap = new ArrayBuffer[(Int, String)]()
        for (item <- transaction) {
          for (i <- 0 until headers.length) {
            if (headers(i).name.equals(item)) {
              itemMap += i -> item
            }
          }
        }
        itemMap.toArray.sortWith(_._1 < _._1).foreach(sortedTransaction += _._2)
        sortedTransaction
      }

      def addTreeNodes(transaction: ArrayBuffer[String], parent: TreeNode) {
        while (!transaction.isEmpty) {
          val leaf = new TreeNode(name = transaction.head, count = 1, parent = parent)
          parent.children += leaf

          var i = 0
          var break_flag = true
          while (break_flag && i < headers.length) {
            var treeNode = headers(i)
            if (treeNode.name.equals(leaf.name)) {
              while (treeNode.nextSibling != null) {
                treeNode = treeNode.nextSibling
              }
              treeNode.nextSibling = leaf
              break_flag = true
            }
            i += 1
          }

          transaction.remove(0)
          addTreeNodes(transaction, leaf)
        }
      }

      val root = new TreeNode()

      for (transaction <- transactions) {
        val sortedTransaction = sortTransaction(transaction)
        var pTreeNode: TreeNode = root
        var qTreeNode: TreeNode = null
        while (!root.children.isEmpty && sortedTransaction.nonEmpty && pTreeNode.findChildren(sortedTransaction.head) != null) {
          qTreeNode = pTreeNode.findChildren(sortedTransaction.head)
          qTreeNode.count += 1
          pTreeNode = qTreeNode
          sortedTransaction.remove(0)
        }
        addTreeNodes(sortedTransaction, pTreeNode)
      }

      root
    }

  }

}
