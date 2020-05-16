import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._


object Graph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph Processing")
    val sc = new SparkContext(conf)

    var graph = sc.textFile(args(0)).map(line => { val a = line.split(",")
                                                 (a(0).toLong, a(0).toLong, a.tail.map(_.toLong).toList)
                                                 })
    val first = graph.map(group => (group._1, group))
    for (i <- 1 to 5)
    {
      graph = graph.flatMap { case (a, b, c) => (a, b) :: c.map(d => (d, b)) }
              .reduceByKey(min)
              .join(first)
              .map(group => (group._2._2._2, group._2._1, group._2._2._3))
    }

    val res = graph.map(group => (group._2, 1))
    val cal = res.reduceByKey(_+_).sortByKey()
    val total = cal.map{ case (p,q) => p + " " + q }.collect()
    total.foreach(println)
    
  }
}