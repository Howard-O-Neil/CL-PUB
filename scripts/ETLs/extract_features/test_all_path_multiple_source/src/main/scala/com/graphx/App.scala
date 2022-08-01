package graphx

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.control._
import org.apache.spark.sql.{functions => sparkf}
import org.apache.spark.{sql => sparkSQL}
import org.apache.spark.sql.{types => sparkSQL_T}

object App {
    val src = 5L
    val dst = 3L
    val dst_dir = "gs://clpub/data_lake/arnet/tables/test_all_path/merge-0"

    def main(args: Array[String]) = {
        val spark = (SparkSession.builder.getOrCreate())
        val sc = spark.sparkContext

        // Create an RDD for the vertices
        val users: RDD[(VertexId, Double)] =
            sc.parallelize(Seq((1L, 0D), (2L, 0D),
                                (3L, 0D), (4L, 0D),
                                (5L, 0D), (6L, 0D),
                                (7L, 0D), (8L, 0D),
                                (9L, 0D), (10L, 0D),
                                (11L, 0D), (12L, 0D),
                                (13L, 0D), (14L, 0D),
                                (15L, 0D), (16L, 0D)))

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

        var g: Graph[String, Double] = graph.mapVertices((id, attr) => {
            (id.toString + ":" + 0D.toString)
        })

        val loop = new Breaks
        loop.breakable {
            while (true) {
                val msgs = g.aggregateMessages[String] (
                    triplet => {
                        var path: List[(String, Double)]      = List()

                        for (item <- triplet.srcAttr.split(";")) {
                            var Array(path_str, weight) = item.split(":")

                            path = path :+ (
                                (path_str + "," + triplet.dstId.toString),
                                (weight.toDouble + triplet.attr)
                            )
                        }

                        for (dst_item <- triplet.dstAttr.split(";")) {
                            path = path.filter(x => !(x._1.startsWith(dst_item.split(":")(0))))
                        }

                        println("[===== Path length =====]")
                        println(path.length)

                        if (path.length > 0)
                            triplet.sendToDst(
                                path.map(x => x._1 + ":" + x._2.toString).reduce((a, b) => a + ";" + b))
                    },
                    (a, b) => (a + ";" + b)
                )

                println("[===== Loop =====]")
                println(msgs.count())

                if (msgs.count() <= 0) loop.break

                g = g.ops.joinVertices(msgs) (
                    (id, oldAttr, newDist) => {
                        (oldAttr + ";" + newDist)
                    }
                )
            }
        }

        println(g.vertices.filter(x => x._1 == src || x._1 == dst).collect().mkString("\n"))
    }
}
