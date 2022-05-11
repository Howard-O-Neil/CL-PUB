package graphx

import org.apache.spark.sql.SparkSession

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


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
        val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                            (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

        // Create an RDD for edges
        val relationships: RDD[Edge[String]] =
        sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                            Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

        // Define a default user in case there are relationship with missing user
        val defaultUser = ("John Doe", "Missing")

        // Build the initial Graph
        val graph = Graph(users, relationships, defaultUser)
    }
}