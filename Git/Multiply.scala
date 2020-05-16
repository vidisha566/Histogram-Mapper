import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
  	val conf = new SparkConf().setAppName("Matrix Multiplication")
  	val sc = new SparkContext(conf)

  	val matrixM = sc.textFile(args(0)).map(line => {
  							val readLine = line.split(",")
  							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
  						} )

  	val matrixN = sc.textFile(args(1)).map(line => {
  							val readLine = line.split(",")
  							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
  						} )

  	val multiply = matrixM.map(matrixM => (matrixM._2, matrixM)).join(matrixN.map(matrixN => (matrixN._1, matrixN)))
			.map { case (k, (matrixM, matrixN)) =>
				((matrixM._1, matrixN._2), (matrixM._3 * matrixN._3))}.reduceByKey(_+_).sortByKey(true).map{ case (key,v) => key._1 + " " + key._2 + " " + v}
				
	//multiply.foreach(println)
	
	multiply.saveAsTextFile(args(2))
  	sc.stop()

	}
}
