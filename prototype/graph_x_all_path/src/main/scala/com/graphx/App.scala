package graphx
import java.io._

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.control._

object App {
    def writeToFile(message: String) = {
        val out: BufferedWriter = new BufferedWriter(
            new FileWriter("/recsys/prototype/graph_x_all_path/output.txt", true));

        out.write(message)
        out.close()
    }

    def filterMessage(src: List[List[VertexId]], dst: List[List[VertexId]]): List[List[VertexId]] = {
        var result: List[(List[VertexId], Int)] = List()
        // Suppose we send full src to dst
        // Then check to filter
        result = result ::: src.zipWithIndex
        
        for (dstItem <- dst) {

            for (srcIdx <- 0 to src.size - 1) {
                val srcItem = src(srcIdx)

                if (dstItem.size <= srcItem.size) {
                    var similarCount = 0
                    for (i <- 0 to dstItem.size - 1) {
                        if (srcItem(i) == dstItem(i)) similarCount += 1
                    }

                    if (similarCount == dstItem.size)
                        result = result.filter(item => item._2 != srcIdx)
                }
            }
        }
        return result.map(item => item._1)
    }

    def main(args: Array[String]) = {
        
        val spark = SparkSession.builder
            .config("spark.app.name", "Recsys")
            .config("spark.master", "local[*]")
            .config("spark.submit.deployMode", "client")
            .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar")
            .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
            .getOrCreate()

        val sc = spark.sparkContext

        // Create an RDD for the vertices
        val users: RDD[(VertexId, (String, Double))] =
            sc.parallelize(Seq((1L, ("A", 0D)), (2L, ("B", 0D)),
                                (3L, ("C", 0D)), (4L, ("D", 0D)),
                                (5L, ("E", 0D)), (6L, ("F", 0D)),
                                (7L, ("G", 0D)), (8L, ("H", 0D)),
                                (9L, ("I", 0D))))

        // Create an RDD for edges
        val relationships: RDD[Edge[Double]] = 
            sc.parallelize(Seq(
                // AB, AC, AD, AF
                Edge(1L, 2L, 0D), Edge(2L, 1L, 0D),
                Edge(1L, 3L, 0D), Edge(3L, 1L, 0D),
                Edge(1L, 4L, 0D), Edge(4L, 1L, 0D),
                Edge(1L, 6L, 0D), Edge(6L, 1L, 0D),
                // BC, BE, BF
                Edge(2L, 3L, 0D), Edge(3L, 2L, 0D),
                Edge(2L, 5L, 0D), Edge(5L, 2L, 0D),
                Edge(2L, 6L, 0D), Edge(6L, 2L, 0D),
                // EF
                Edge(5L, 6L, 0D), Edge(6L, 5L, 0D),
                // FG
                Edge(6L, 7L, 0D), Edge(7L, 6L, 0D),
                // CG, CI, CH
                Edge(3L, 7L, 0D), Edge(7L, 3L, 0D),
                Edge(3L, 9L, 0D), Edge(9L, 3L, 0D),
                Edge(3L, 8L, 0D), Edge(8L, 3L, 0D),
                // GH
                Edge(7L, 8L, 0D), Edge(8L, 7L, 0D),
                // DI
                Edge(4L, 9L, 0D), Edge(9L, 4L, 0D)))

        val graph = Graph(users, relationships)

        val src: VertexId = 1L
        val dst: VertexId = 8L

        var g: Graph[(String, Int, List[List[VertexId]]), Double] =
            graph.mapVertices((id, attr) =>
                if (id == src) (attr._1, 1, List(List(id)))
                else (attr._1, 0, List()))

        val loop = new Breaks

        loop.breakable {
            while (true) {
                val msgs = g.aggregateMessages[List[List[VertexId]]] (
                    triplet => 
                        if (triplet.dstAttr._2 == 0) {
                            var result: List[List[VertexId]] = List()
                            
                            for (itemList <- triplet.srcAttr._3) {
                                val newList = itemList ::: List(triplet.dstId)
                                result = result ::: List(newList)
                            }

                            val sendMessage = filterMessage(result, triplet.dstAttr._3)

                            if (sendMessage.size > 0)
                                triplet.sendToDst(result)
                        },
                    (a, b) => a ::: filterMessage(b, a)
                )
                if (msgs.count() <= 0) loop.break
                
                g = g.ops.joinVertices(msgs) (
                    (id, oldAttr, newDist) =>
                        (oldAttr._1, oldAttr._2, oldAttr._3 ::: filterMessage(newDist, oldAttr._3))
                )
            }
        }
        writeToFile(g.vertices.collect.mkString("\n"))
    }
}