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

    def filterMessage(src: List[List[VertexId]], dst: List[List[VertexId]]): (List[List[VertexId]], List[Int]) = {
        var result: List[(List[VertexId], Int)] = List()
        var removedIdx: List[Int]               = List()
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

                    if (similarCount == dstItem.size) {
                        removedIdx = removedIdx ::: List(srcIdx)
                        result = result.filter(item => item._2 != srcIdx)
                    }
                }
            }
        }
        return (result.map(item => item._1), removedIdx)
    }

    def filterSrcIdx(src: List[Double], idx: List[Int]): List[Double] = {
        var result: List[(Double, Int)] = List()
        result = result ::: src.zipWithIndex

        for (srcIdx <- idx) {
            result = result.filter(item => item._2 != srcIdx)
        }
        return result.map(item => item._1)
    }

    def main(args: Array[String]) = {
        val spark = SparkSession.builder.getOrCreate()
        val sc = spark.sparkContext

        // Create an RDD for the vertices
        val users: RDD[(VertexId, (String, Double))] =
            sc.parallelize(Seq((1L, ("A", 0D)), (2L, ("B", 0D)),
                                (3L, ("C", 0D)), (4L, ("D", 0D)),
                                (5L, ("E", 0D)), (6L, ("F", 0D)),
                                (7L, ("G", 0D)), (8L, ("H", 0D)),
                                (9L, ("I", 0D)),
                                (10L, ("K", 0D)),
                                (11L, ("U", 0D)),
                                (12L, ("O", 0D)),
                                (13L, ("Z", 0D)),
                                (14L, ("T", 0D)),
                                (15L, ("R", 0D)), (16L, ("S", 0D))))

        // Create an RDD for edges
        val relationships: RDD[Edge[Double]] = 
            sc.parallelize(Seq(
                // AB, AC, AD, AF
                Edge(1L, 2L, 5D), Edge(2L, 1L, 5D),
                Edge(1L, 3L, 5D), Edge(3L, 1L, 5D),
                Edge(1L, 4L, 8D), Edge(4L, 1L, 8D),
                Edge(1L, 6L, 7D), Edge(6L, 1L, 7D),
                // BC, BE, BF
                Edge(2L, 3L, 5D), Edge(3L, 2L, 5D),
                Edge(2L, 5L, 9D), Edge(5L, 2L, 9D),
                Edge(2L, 6L, 3D), Edge(6L, 2L, 3D),
                // EF
                Edge(5L, 6L, 12D), Edge(6L, 5L, 12D),
                // FG
                Edge(6L, 7L, 9D), Edge(7L, 6L, 9D),
                // CG, CI, CH
                Edge(3L, 7L, 7D), Edge(7L, 3L, 7D),
                Edge(3L, 9L, 9D), Edge(9L, 3L, 9D),
                Edge(3L, 8L, 35D), Edge(8L, 3L, 35D),
                // GH
                Edge(7L, 8L, 3D), Edge(8L, 7L, 3D),
                // DI
                Edge(4L, 9L, 6D), Edge(9L, 4L, 6D),
                // KU, KO, KZ
                Edge(10L, 11L, 9D), Edge(11L, 10L, 9D),
                Edge(10L, 12L, 8D), Edge(12L, 10L, 8D),
                Edge(10L, 13L, 3D), Edge(13L, 10L, 3D),
                // ZU
                Edge(13L, 11L, 5D), Edge(11L, 13L, 5D),
                // OU
                Edge(12L, 11L, 6D), Edge(11L, 12L, 6D),
                // RS
                Edge(16L, 15L, 7D), Edge(15L, 16L, 7D)))

        val graph = Graph(users, relationships)

        val connected_groups_t = graph.ops.connectedComponents().vertices
        var connected_groups = connected_groups_t.map(x => (x._2, List(x._1)))

        connected_groups = connected_groups.reduceByKey(
            (a, b) => a ::: b
        )

        // val src: VertexId = 7L
        // val dst: VertexId = 8L

        // val src_connected   = connected_groups.filter(group => group._2.contains(src)).collect()(0)._2
        // val subgraph        = graph.subgraph(vpred = (vid, attr) => src_connected.contains(vid))      

        var g: Graph[(String, Int, List[List[VertexId]], List[Double]), Double] =
            graph.mapVertices((id, attr) => (attr._1, 1, List(List(id)), List(0d)))

        val loop = new Breaks

        loop.breakable {
            val msgs = g.aggregateMessages[(List[List[VertexId]], List[Double])] (
                triplet => {
                    var result: List[List[VertexId]]    = List()
                    var path_w: List[Double]            = List()

                    for (itemList <- triplet.srcAttr._3) {
                        val newList     = itemList ::: List(triplet.dstId)
                        result          = result ::: List(newList)
                    }
                    for (weight <- triplet.srcAttr._4) {
                        val newWeight   = weight + triplet.attr
                        path_w          = path_w ::: List(newWeight)
                    }

                    val (sendMessage, removedIdx)   = filterMessage(result, triplet.dstAttr._3)
                    val sendPathW                   = filterSrcIdx(path_w, removedIdx)

                    if (sendMessage.size > 0)
                        triplet.sendToDst((sendMessage, sendPathW))
                },
                (a, b) => (a._1 ::: b._1, a._2 ::: b._2)
            )
            if (msgs.count() <= 0) loop.break

            g = g.ops.joinVertices(msgs) (
                (id, oldAttr, newDist) =>
                    (oldAttr._1, oldAttr._2, oldAttr._3 ::: newDist._1, oldAttr._4 ::: newDist._2)
            )
        }
        
        // writeToFile("===== Path\n\n")
        // writeToFile(g.vertices.map(item => (item._1, item._2._3)).collect.mkString("\n") + "\n\n")
        // writeToFile("===== Weights\n\n")
        // writeToFile(g.vertices.map(item => (item._1, item._2._4)).collect.mkString("\n") + "\n\n")

        // println("===== Path\n\n")
        // println(g.vertices.map(item => (item._1, item._2._3)).collect.mkString("\n") + "\n\n")
        // println("===== Weights\n\n")
        // println(g.vertices.map(item => (item._1, item._2._4)).collect.mkString("\n") + "\n\n")
    }
}