import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shaohui on 2017/3/16 0016.
  */
object userLocation {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("Userlocation").setMaster("local")
    val sc = new SparkContext(config)

    //((18611132889,CC0710CC94ECC657A8561DE549D940E0),-20160327081100)
    val rdd1 = sc.textFile("D:\\spark-app\\UserLocationLog\\*.log").map(line => {
      val fields = line.split(",")
      val time = if(fields(3).toInt == 1) -(fields(1).toLong) else fields(1).toLong
      ((fields(0),fields(2)),time)
    })
    //(CC0710CC94ECC657A8561DE549D940E0,(116.303955,40.041935))
    val loc_info = sc.textFile("D:\\spark-app\\UserLocationLog\\loc_info.txt").map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      val x = fields(1)
      val y = fields(2)
      (lac,(x,y))
    })
    //((18688888888,CC0710CC94ECC657A8561DE549D940E0),1300)
    val rdd2 = rdd1.reduceByKey(_ + _)
    //(CC0710CC94ECC657A8561DE549D940E0,(18688888888,1300))
    val rdd3 = rdd2.map(line => {
      val num = line._1._1
      val lac = line._1._2
      val time = line._2
      (lac,(num,time))
    })
    //(CC0710CC94ECC657A8561DE549D940E0,((18688888888,1300),(116.303955,40.041935)))
    val rdd4 = rdd3.join(loc_info)
    //(18688888888,16030401EAFB68F1E3CDF819735E1C66,87600,116.296302,40.032296)
    val rdd5 = rdd4.map(t => {
      val lac = t._1
      val num = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (num,lac,time,x,y)
    })

    val rdd6 = rdd5.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    }
    )

    //println(rdd6.collect().toBuffer)
    rdd1.foreach(println)
    //loc_info.foreach(println)
  }

}
