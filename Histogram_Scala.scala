import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("HistogramMap")
    val sc = new SparkContext(conf)
    val ln = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                               (a(0).toShort, a(1).toShort, a(2).toShort) } ) 
    
    val r = ln.map ( red => ((1, red._1),1))
    val g = ln.map ( green => ((2, green._2),1))
    val b = ln.map ( blue => ((3, blue._3),1))

    val i = r.reduceByKey(_+_)
    val j = g.reduceByKey(_+_)
    val k = b.reduceByKey(_+_)
    
    val res = (i.union(j).union(k)).map{ case ((key1,key2), value) => key1 + " " + key2 + " " + value}
    res.collect().foreach(println)
    sc.stop()     
  }
}