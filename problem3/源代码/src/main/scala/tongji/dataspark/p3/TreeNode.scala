package tongji.dataspark.p3

import scala.collection.mutable.ArrayBuffer

/**
 * TreeNode.scala
 */
class TreeNode (val name: String = null, var count: Int = 0, var parent: TreeNode = null, var nextSibling: TreeNode = null, val children: ArrayBuffer[TreeNode] = new ArrayBuffer[TreeNode]) extends java.io.Serializable {
  def findChildren(name: String): TreeNode = {
    children.find(_.name.equals(name)) match {
      case Some(treeNode) => treeNode
      case None => null
    }
  }
}
