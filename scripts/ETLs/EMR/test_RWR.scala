import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.control._
import scala.collection._
import scala.util.Random
import org.apache.spark.{sql => sparkSQL}
import org.apache.spark.sql.{functions => sparkf}
import org.apache.spark.sql.{types => sparkt}

object App extends Serializable {
    def main(args: Array[String]) = {
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
                                (15L, ("R", 0D)), (16L, ("S", 0D)),
                                (17L, ("Q", 0D))))

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
                Edge(16L, 15L, 7D), Edge(15L, 16L, 7D),
                // QA
                Edge(17L, 1L, 9D), Edge(1L, 17L, 9D),
                // Q->E, A->E
                Edge(17L, 5L, 15D), Edge(1L, 5L, 8D)))

        val graph = Graph(users, relationships)

        val total_vertices      = graph.vertices.count().toFloat
        val defaultRestartVec   = 1f / total_vertices
        val defaultRank         = 1f
        val resProb             = 0.15f
        val damFac              = 1f - resProb
        val maxStep             = 15

        var g = graph.partitionBy(
            PartitionStrategy.EdgePartition2D).mapVertices((id, attr) => (defaultRank, 0, 0))

        g = g.outerJoinVertices(graph.inDegrees) {
            (vid, vdata, deg) => (vdata._1, deg.getOrElse(0), vdata._3)
        }
        g = g.outerJoinVertices(graph.outDegrees) {
            (vid, vdata, deg) => (vdata._1, vdata._2, deg.getOrElse(0))
        }

        g = g.mapVertices((id, attr) => {
            if (attr._2 == 0 || attr._3 == 0) {
                ((resProb * defaultRestartVec), attr._2, attr._3)
            } else (attr._1, attr._2, attr._3)
        })
        
        var step = 0
        var loop = new Breaks
        loop.breakable {
            while (true) {
                val msgs = g.aggregateMessages[Float] (
                    triplet => {
                        triplet.sendToDst(triplet.srcAttr._1 / triplet.srcAttr._3)
                    },
                    (a, b) => a + b
                )

                g = g.ops.joinVertices(msgs) {
                    (id, oldAttr, newAttr) => {
                        ((resProb * defaultRestartVec) + (damFac * newAttr), oldAttr._2, oldAttr._3)
                    }
                }

                step += 1
                if (step >= maxStep) {
                    loop.break
                }
            }
        }
        println(g.vertices.collect().mkString("\n"))
    }
}

val app = App
app.main(Array("main"))

// ===== Result =====

//      (1,(0.19969358,5,6))
//      (2,(0.19989227,4,4))
//      (3,(0.21465953,5,5))
//      (4,(0.08030682,2,2))
//      (5,(0.1532013,4,2))
//      (6,(0.2027547,4,4))
//      (7,(0.14031377,3,3))
//      (8,(0.09300676,2,2))
//      (9,(0.0865284,2,2))
//      (10,(0.16816047,3,3))
//      (11,(0.16816047,3,3))
//      (12,(0.1139182,2,2))
//      (13,(0.1139182,2,2))
//      (14,(0.00882353,0,0))
//      (15,(0.14103931,1,1))
//      (16,(0.14103931,1,1))
//      (17,(0.040036134,1,2))