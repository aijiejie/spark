import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shaohui on 2017/3/17 0017.
  */

//sort =>规则 先按faveValue，比较年龄
//name,faveValue,age

object OderingContext  {
  implicit val orderedGirl = new Ordering[Girl1] {
    override def compare(x: Girl1, y: Girl1): Int = {
      if(x.face == y.face){
        x.age - y.age
      }else{
        x.face - y.face
      }
    }
  }

}

object CustomSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)))
    //第一种方法
    val rdd2 = rdd1.sortBy(x => new Girl1(x._2,x._3))
    //第二种方法
    import OderingContext._
    val rdd3 = rdd1.sortBy(x => new Girl1(x._2,x._3))
    rdd2.foreach(println)
    rdd3.foreach(println)
  }
}
/*第一种方法，直接新建girl类继承ordered类，在类中实现compare方法*/
case class Girl(face : Int, age :Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if(this.face == that.face){
      that.age - this.age
    }else{
      this.face - that.face
    }
  }
}
/*第二种方法，用隐式转换实现，新建object对象，在该对象中新建ordering类，实现compare方法*/
case class Girl1(face : Int, age : Int) extends Serializable
