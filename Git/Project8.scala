import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object GraphComponents {
	def main ( args: Array[String] ){
		val conf=new SparkConf().setAppName("Graph")
		val sc = new SparkContext(conf)

		val mapedge : RDD[Edge[Long]] = sc.textFile(args(0)).map(line => { val (n, side)=line.split(",").splitAt(1)
		  (n(0).toLong,side.toList.map(_.toLong))}).flatMap(first=> first._2.map(second=>(first._1,second))).map(n=>Edge(n._1,n._2,n._1))

		val graph: Graph[Long,Long]=Graph.fromEdges(mapedge,"Property").mapVertices((id,_)=>id)

		val join = graph.pregel(Long.MaxValue,5)((id,group1,group2)=> math.min(group1,group2),
			count=>{ if(count.attr<count.dstAttr){ Iterator((count.dstId,count.attr))}
			else if ((count.srcAttr<count.attr)){ Iterator((count.dstId,count.srcAttr))}
			else { Iterator.empty }}, 
			(first,second)=>math.min(first,second))
				
		    val output = join.vertices.map(g=>(g._2, 1))
		    val total = output.reduceByKey(_+_)
		    val res= total.sortByKey()
		    val fin = res.map( x => x._1.toString + " " + x._2.toString )
		    fin.collect().foreach(println)
	}
}