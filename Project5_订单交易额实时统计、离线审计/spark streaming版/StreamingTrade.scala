package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 从订单服务器获取订单，服务器每秒生成一条订单，简化订单格式(商品码\t单价\t购买数量)
  * 1、实时显示总销售额
  * 2、打印最近1分钟、5分钟、15分钟的销售总额(其实一样)
  * 3、打印最近1小时内销售总额前10的商品
  */
object StreamingTrade {
  def main(args: Array[String]): Unit = {
    //1、创建配置对象
    val conf: SparkConf = new SparkConf()
    conf.setAppName(StreamingOrder.getClass.getSimpleName)
    conf.setMaster("local[2]")

    //2、构造StreamingContext，第1个是配置对象，第2个是时间间隔
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("D:/trade")
    //3、定义接收器
    val textStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.100", 5656, StorageLevel.MEMORY_ONLY)

    //4、业务逻辑
    val wc: DStream[(String, Double)] = textStream.map(line => {
      val split: Array[String] = line.split("\t")
      // (商品码，销售额)
      (split(0), split(1).toDouble * split(2).toInt)
    })
    // 实时销售额，调用状态更新函数更新状态
    wc.map { case (k, v) => ("实时销售总额", v) }.updateStateByKey(updateFunc).print()

    // 1、5、15分钟销售额
    wc.reduceByKeyAndWindow((x: Double, y: Double) => x + y, Seconds(60), Seconds(1)).map(_._2).reduce(_ + _).print()
    wc.reduceByKeyAndWindow((x: Double, y: Double) => x + y, Seconds(60 * 5), Seconds(1)).map(_._2).reduce(_ + _).print()
    wc.reduceByKeyAndWindow((x: Double, y: Double) => x + y, Seconds(60 * 15), Seconds(1)).map(_._2).reduce(_ + _).print()

    // Top10 1小时内的销售总额前10的商品
    val top10DStream: DStream[(String, Double)] = wc.reduceByKeyAndWindow((x: Double, y: Double) => x + y, Seconds(60 * 60), Seconds(1))
    top10DStream.foreachRDD {
      _.sortBy(_._2, false).take(10).foreach(println)
    }

    //5、启动流计算
    ssc.start()

    //6、等待程序结束
    ssc.awaitTermination()
  }

  /**
    * 状态更新
    *
    * @param value
    * @param status
    * @return
    */
  def updateFunc(value: Seq[Double], status: Option[Double]) = {
    //获取当前状态
    val thisStatus: Double = value.sum
    //获取上一状态
    val lastStatus: Double = status.getOrElse(0)
    Some(thisStatus + lastStatus)
  }

}
