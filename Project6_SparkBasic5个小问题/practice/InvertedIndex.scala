package practice

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 需求：倒排索引
  * 原始数据
  * cx1|a,b,c,d,e,f
  * cx2|c,d,e,f
  * cx3|a,b,c,f
  * cx4|a,b,c,d,e,f
  * cx5|a,b,e,f
  * cx6|a,b,c,d
  * cx7|a,b,c,f
  * cx8|d,e,f
  * cx9|b,c,d,e,f
  * 结果数据
  * d|cx1,cx2,cx4,cx6,cx8,cx9
  * e|cx1,cx2,cx4,cx5,cx8,cx9
  * a|cx1,cx3,cx4,cx5,cx6,cx7
  * b|cx1,cx3,cx4,cx5,cx6,cx7,cx9
  * f|cx1,cx2,cx3,cx4,cx5,cx7,cx8,cx9
  * c|cx1,cx2,cx3,cx4,cx6,cx7,cx9
  */
object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = sparkutil.Util.sc
    val rdd: RDD[String] = sc.textFile("D:/index.txt")
    //1、以"|"切割字符串，返回Tuple(value,index)
    val rdd1: RDD[(String, String)] = rdd.flatMap(line => {
      val split: Array[String] = line.split("\\|")
      var list: List[(String, String)] = List()
      for (word <- split(1).split(",")) {
        list = (word, split(0)) :: list
      }
      list
    })
    //2、reduceByKey
    rdd1.reduceByKey((x, y) => {
      x + "," + y
    }).foreach(line => {
      println(line._1 + "|" + line._2)
    })
  }
}
