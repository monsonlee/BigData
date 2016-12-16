package practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 需求：
  * 文件sheet.txt内容如下
  * 00001	1
  * 00002	2
  * 文件product.txt内容如下
  * 1	皮鞋
  * 2	衣服
  * 希望输出结果
  * 00001	皮鞋
  * 00002	衣服
  * Created by Administrator on 2016/12/15.
  */
object DicConn {
  def main(args: Array[String]): Unit = {
    var sc: SparkContext = sparkutil.Util.sc
    //1、读取文件内容
    val rdd: RDD[String] = sc.textFile("D:/sheet.txt")
    val rdd1: RDD[String] = sc.textFile("D:/product.txt")
    //2、rdd中的Key,Value对调后组成Tuple返回
    val sheetRDD: RDD[(String, String)] = rdd.map(x => {
      val split: Array[String] = x.split("\t")
      (split(1), split(0))
    })
    //3、rdd1中的Key,Value组成Tuple返回
    val productRDD: RDD[(String, String)] = rdd1.map(x => {
      val split: Array[String] = x.split("\t")
      (split(0), split(1))
    })
    //4、sheetRDD与productRDD进行join连接
    //5、利用map取出value
    //6、sortByKey排序并打印
    sheetRDD.join(productRDD).map(_._2).sortByKey().foreach(println(_))
  }
}
