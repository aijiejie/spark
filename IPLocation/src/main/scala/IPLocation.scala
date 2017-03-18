import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shaohui on 2017/3/18 0018.
  */
object IPLocation {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(ip:Long, lines:Array[(String,String,String)]):Int  = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if (ip <= lines(middle)._2.toLong && ip >= lines(middle)._1.toLong)
        return middle
      if (ip > lines(middle)._2.toLong)
        low = middle + 1
      else
        high = middle - 1
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("IPLocation")
    val sc = new SparkContext(conf)

    val IpRules = sc.textFile("D:\\IPLocation\\iprules.txt").map(line => {
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num,end_num,province)
    })

    val IPRulesArray = IpRules.collect()
    val IPRulesBroadcast = sc.broadcast(IPRulesArray)

    val ip = sc.textFile("D:\\IPLocation\\ip.txt").map( line => {
      line.split("\\|")(1)
    })

    val rs = ip.map(line => {
      val ip_num = ip2Long(line)
      val index = binarySearch(ip_num,IPRulesBroadcast.value)
      val info = IPRulesBroadcast.value(index)
      info
    })
    val rs_reduce = rs.map(x => {
      val city = x._3
      (city,1)
    }).reduceByKey(_+_).sortBy(_._2,false)
    rs_reduce.foreach(println(_))
    sc.stop()
  }
}
