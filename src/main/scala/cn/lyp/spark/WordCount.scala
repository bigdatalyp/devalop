package cn.lyp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val sc: SparkContext = new SparkContext(config)
    val textRDD: RDD[(Int, Int)] = sc.makeRDD(List((1, 2), (3, 4), (5, 6), (7, 8)))

    val ptRDD: RDD[(Int, Int)] = textRDD.partitionBy(new MyPartition(4))
    ptRDD.saveAsTextFile("out")
  }


  class MyPartition(partitions:Int) extends Partitioner{
    override def numPartitions: Int = {
      partitions
    }

    override def getPartition(key: Any): Int = {
      key.hashCode()%3
    }
  }
}
