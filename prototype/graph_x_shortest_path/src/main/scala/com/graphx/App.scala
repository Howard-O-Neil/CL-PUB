package graphx

import org.apache.spark.sql.SparkSession

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators

object App {
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
        val users: RDD[(VertexId,  (String, Double))] =
            sc.parallelize(Seq((1L, ("A", 0D)), (2L, ("B", 0D)),
                                (3L, ("C", 0D)), (4L, ("D", 0D)),
                                (5L, ("E", 0D)), (6L, ("F", 0D)),
                                (7L, ("G", 0D)), (8L, ("H", 0D)),
                                (9L, ("I", 0D))))

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
                Edge(4L, 9L, 6D), Edge(9L, 4L, 6D)))

        val graph = Graph(users, relationships)

        // // Check attr structure
        // graph.triplets.collect().foreach(triplet => println(triplet.dstAttr))

        // // Some printing techniques
        // print(graph.vertices.collect().mkString("\n"))
        // print(graph.edges.collect().mkString("\n"))

        val sourceIds: List[VertexId] = List(1L)
        val initial_graph = graph.mapVertices((id, attr) => 
            if (sourceIds.contains(id)) (id, 0D) else (id, Double.PositiveInfinity))

        val sssp = initial_graph.pregel(Double.PositiveInfinity)(
            (id, dist, newDist) => (id, math.min(dist._2, newDist)), // Vertex Program
            triplet => {
                // Attribute = property
                if (triplet.srcAttr._2 + triplet.attr < triplet.dstAttr._2) {
                    Iterator((triplet.dstId, triplet.srcAttr._2 + triplet.attr))
                } else {
                    Iterator.empty
                }
            },
            (a, b) => math.min(a, b) // Merge Message
        )
        println(sssp.vertices.collect.mkString("\n"))            
    }
}