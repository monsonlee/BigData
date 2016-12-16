package sparkutil

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark工具类
  * Created by Administrator on 2016/12/15.
  */
object Util {
  def conf: SparkConf = {
    System.setProperty("hadoop.home.dir", "D:/Program Files/hadoop_winutils")
    System.setProperty("spark.master", "local")
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Util.getClass.getSimpleName)
    conf
  }

  def sc: SparkContext = new SparkContext(conf)

  /**
    * 加载文件或者文件夹（本地或Hdfs），生成RDD
    *
    * @param path
    * @return
    */
  def loadFile(path: String): RDD[String] = {
    val rdd: RDD[String] = Util.sc.textFile(path)
    rdd
  }

  /**
    * 加载文件或文件夹，生成RDD[(String,String)]，第一个是文件名，第二个是文件内容
    *
    * @param path
    * @return
    */
  def loadWholeFile(path: String): RDD[(String, String)] = {
    val rdd: RDD[(String, String)] = Util.sc.wholeTextFiles(path)
    rdd
  }
}
