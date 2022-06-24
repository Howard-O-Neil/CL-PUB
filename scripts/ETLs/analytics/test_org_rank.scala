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

object App {
    // RWR max Step concept
    // I haven't not yet found good formula on calculating MaxStep or MaxSuperStep
    var maxStep         = 20
    var step            = 0

    var result_rdd : RDD[(Long, (Float, Int))] = sc.emptyRDD

    def main(args: Array[String]) = {
        val sc = spark.sparkContext

        // Create an RDD for the vertices
        val users: RDD[(VertexId, (String, Double))] =
            sc.parallelize(Seq((1L, ("A", 0D)), (2L, ("C", 0D)),
                                (3L, ("E", 0D)), (4L, ("D", 0D)),
                                (5L, ("F", 0D)), (6L, ("G", 0D)),
                                (7L, ("T", 0D)), (8L, ("K", 0D)),
                                (9L, ("H", 0D))))

        // Create an RDD for edges
        val relationships: RDD[Edge[Double]] = 
            sc.parallelize(Seq(
                // AC
                Edge(1L, 2L, 7D), Edge(2L, 1L, 7D),
                // AD
                Edge(1L, 4L, 3D), Edge(4L, 1L, 3D),
                // AT
                Edge(1L, 7L, 1D), Edge(7L, 1L, 1D),
                // AK
                Edge(1L, 8L, 2D), Edge(8L, 1L, 2D),
                // DC
                Edge(2L, 4L, 8D), Edge(4L, 2L, 8D),
                // CE
                Edge(2L, 3L, 3D), Edge(3L, 2L, 3D),
                // EF
                Edge(3L, 5L, 9D), Edge(5L, 3L, 9D),
                // EG
                Edge(3L, 6L, 5D), Edge(6L, 3L, 5D),
                // GG, HH
                Edge(6L, 6L, 3D), Edge(9L, 9L, 7D)))

        val graph = Graph(users, relationships)

        val total_vertices  = graph.vertices.count().toFloat
        val default_ranking = 0D

        var g = graph.mapVertices((id, attr) => (default_ranking, 0D, 0))

        var sum_neighbor = graph.mapVertices((id, attr) => 0D)

        val sum_neighbor_msgs = sum_neighbor.aggregateMessages[Double] (
            triplet => {
                triplet.sendToDst(triplet.attr)
            },
            (a, b) => a + b
        )
        sum_neighbor = sum_neighbor.ops.joinVertices(sum_neighbor_msgs) {
            (id, oldAttr, newDist) => oldAttr + newDist
        }

        println(sum_neighbor.vertices.collect().mkString("\n"))

        // g = g.outerJoinVertices(sum_neighbor.vertices) {
        //     (id, attr, sum_neighbor) => (attr._1, sum_neighbor.getOrElse(1D), attr._3)
        // }
        // g = g.outerJoinVertices(graph.outDegrees) {
        //     (id, attr, deg) => (attr._1, attr._2, deg.getOrElse(0))
        // }

        // val msgs = g.aggregateMessages[Double] (
        //     triplet => {
        //         if (triplet.srcAttr._3 <= 1) {
        //             triplet.sendToDst(triplet.attr / total_vertices)    
        //         } else {
        //             triplet.sendToDst(triplet.attr / triplet.srcAttr._2)
        //         }
        //     },
        //     (a, b) => a + b
        // )
        // g = g.ops.joinVertices(msgs) {
        //     (id, oldAttr, newDist) => (oldAttr._1 + newDist, oldAttr._2, oldAttr._3)
        // }

        // println(g.vertices.collect().mkString("\n"))
    }
}

val app = App
app.main(Array("main"))

