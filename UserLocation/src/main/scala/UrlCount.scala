import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by shaohui on 2017/3/17 0017.
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\spark-app\\UserLocationdata\\itcast.log").map(line => {
      val fields = line.split("\t")
      val url = fields(1)
      (url,1)
    })
    //(http://php.itcast.cn/php/course.shtml,459)
    val rdd2 = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map(t => {
      val host = new URL(t._1).getHost
      (host,(t._1,t._2))
    })

    val host = rdd3.map(t => t._1).distinct().collect()
    val hostPartitioner = new HostPartioner(host)

    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.iterator
    })
    rdd3.foreach(println)
    println(host.toBuffer)
    rdd4.saveAsTextFile("D:\\spark-app\\UserLocationdata\\out")
    sc.stop()
  }

}

class HostPartioner(host : Array[String]) extends Partitioner{
  val mapHost = new mutable.HashMap[String,Int]()
  var count = 0
  for(i <- host) {
    mapHost += (i -> count)
    count += 1
  }
  override def numPartitions: Int = host.length

  override def getPartition(key: Any): Int = {

    mapHost.getOrElse(key.toString,0)
  }
}
