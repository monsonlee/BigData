package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：词频计数 TopN
  * 思路：
  * 1、先把所有单词分割
  * 2、再给所有单词赋值(key,1)
  * 3、再用reduceByKey进行汇总
  * 4、利用sortBy(value,false)进行倒序
  * 5、利用take取前几位
  * 6、froeach遍历打印
  * Created by Administrator on 2016/12/15.
  */
object TopWordCount {

  def main(args: Array[String]): Unit = {
    var sc: SparkContext = sparkutil.Util.sc
    sc.textFile("D:/1.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(3).foreach(println(_))
  }

}
