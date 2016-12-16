package practice

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 需求：
  * 原始数据
  * 2010-05-04 12:50,10,10,10
  * 2010-05-05 13:50,20,20,20
  * 2010-05-06 14:50,30,30,30
  * 2010-05-05 13:50,20,20,20
  * 2010-05-06 14:50,30,30,30
  * 2010-05-04 12:50,10,10,10
  * 2010-05-04 11:50,10,10,10
  * 结果数据
  * 2010-05-04 11:50,10,10,10
  * 2010-05-04 12:50,20,20,20
  * 2010-05-05 13:50,40,40,40
  * 2010-05-06 14:50,60,60,60
  * 思路：
  * 分组、计算
  * Created by Administrator on 2016/12/16.
  */
object GroupSum {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = sparkutil.Util.sc
    val rdd: RDD[String] = sc.textFile("D:/data.txt")
    //1、先将数据以","切割，然后返回Tuple
    val rdd1: RDD[(String, Array[String])] = rdd.map(x => {
      val split: mutable.ArrayOps[String] = x.split(",", 2) //切割长度为2
      (split(0), split(1).split(","))
    })
    //2、分组、计算
    rdd1.reduceByKey((x, y) => {
      val i0: Int = x(0).toInt + y(0).toInt
      val i1: Int = x(1).toInt + y(1).toInt
      val i2: Int = x(2).toInt + y(2).toInt
      Array(i0 + "," + i1 + "," + i2)
    }).map(x => {
      x._1 + "," + x._2.mkString(",")
    }).foreach(println(_))
  }
}
