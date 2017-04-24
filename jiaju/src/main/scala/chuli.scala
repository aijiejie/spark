import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shaohui on 2017/4/18 0018.
  */
object chuli {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("chuli").setMaster("local")
    val sc = new SparkContext(conf);
    val begin = sc.textFile("C:\\Users\\少辉\\Downloads\\power.txt").map(_.split(";").toBuffer)
    //val filter_begin = begin.filter(a=> a(0) == "20/8/2007")
    val filter_begin = begin.filter(a => a.size == 9 )
    val a = filter_begin.map(fields =>{
      val date = fields(0)
      val time = fields(1)
      val biao1 = fields(6)
      val biao2 = fields(7)
      val biao3 = Some (fields(8)).getOrElse()
      ((date,time),(biao1,biao2,biao3))
    })
    //((5/6/2008,00:00:00),(0.000,0.000,1.000))
    val b = a.map(line => {
      val mini = line._1._2.split(":")
      var min_10 = ""
        min_10 = mini(0) + mini(1).substring(0, 1)
        (line._1._1.toString + "-" + min_10,(line._2._1.toDouble,line._2._2.toDouble,line._2._3.toString.toDouble))
    })

    val c = b.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3)).sortBy(_._1,true)
    //(22/8/2007-170,(0.0,0.0,0.0))
    val d = c.map(x=>{
      var s = new String()
      val kitchen = x._2._1
      val laundry = x._2._2
      val water_heater = x._2._3
      if(kitchen > 90 && kitchen < 160) s =s+"A"
      if(kitchen > 160) s =s+"B"
      if(laundry>180 && laundry<280) s=s+"C"
      if(laundry>=280) s+"D"
      if(water_heater>170) s =s+"E"
      (x._1,s)
    })
    //println("/////////////////d")
    //d.take(300).foreach(println)
    //(20/8/2007-152,BE)
    val e = d.map(x => {
      val k = x._1.split("-")(0) + "/" + x._1.split("-")(1).substring(0,2)
      (k,x._2)
    })
    val f = e.reduceByKey(_+_)
    //println("/////////////////f")
    //f.take(300).foreach(println)
    //(1/4/2007/08,EEEEEE)
    val g = f.filter(a => (a._1.split("/")(0).toInt <= 31) && (a._1.split("/")(1).toInt == 12)  && (a._1.split("/")(2).toInt == 2008) && (a._1.split("/")(3).toInt >=7 && a._1.split("/")(3).toInt <=11)).sortBy(_._1)
    /**
    (1/4/2007/07,E)
    (1/4/2007/08,EEEEEE)
    (1/4/2007/09,EEEEEE)
    (1/4/2007/10,EEEEEE)
    (1/4/2007/11,EEEEEE)
    (14/10/2009/08,EBEBEEEE)
    (8/3/2010/12,BEBEAEAEAEE)
      */
    val h = g.map(x => {
      val items = x._1.split("/")
      val key = items(0) + "/" + items(1) + "/" + items(2) + "-晚峰"
      (key,x._2 + "/")
    }).sortBy(_._1).reduceByKey(_+_)
    //println("////////////////h")
    //h.take(300).foreach(println)
    //(17/4/2007-峰,EEEE/EEEEEE/EEEEEE/EEEEEE/EEE/)
    val i = h.mapValues(v=>{
      val items = v.split("/")
      var value = ""
      for(a <- items){
        if(a.contains("A")) value = value.concat("A")
        if(a.contains("B")) value = value.concat("B")
        if(a.contains("C")) value = value.concat("C")
        if(a.contains("D")) value = value.concat("D")
        if(a.contains("EEE")) value = value.concat("E")
        value = value + "/"
      }
      (value)
    })
    i.repartition(1).map(_._2).saveAsTextFile("D:\\spark-app\\jiaju\\src\\output\\output-2008-12-morning-peak")
    //f.take(1000).foreach(println)
    //println("////////////////i")
    //i.take(300).foreach(println)
    sc.stop()
  }


}
