package graphx

import org.apache.spark.sql.SparkSession

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators

object Sample {
    def sample(args: Array[String]) = {
        val spark = SparkSession.builder
            .config("spark.app.name", "Recsys")
            .config("spark.master", "local[*]")
            .config("spark.submit.deployMode", "client")
            .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar")
            .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
            .getOrCreate()

        val sc = spark.sparkContext

        val users: RDD[(VertexId, List[String])] =
            sc.parallelize(Seq((3L, List("rxin", "student", "normal")), (7L, List("jgonzal", "postdoc", "normal")),
                                (5L, List("franklin", "prof", "normal")), (2L, List("istoica", "prof", "normal"))))

        // Create an RDD for edges
        val relationships: RDD[Edge[String]] =
        sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                            Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
        
        // Define a default user in case there are relationship with missing user
        val defaultUser = List("John Doe", "Missing", "normal")

        // Build the initial Graph
        val graph = Graph(users, relationships, defaultUser)

        // Check more about case syntax in scala
        // In this case, scala traverse all vertex, with each vertex format as (id, (name, pos))
        // id, name, pos will be replace by value from vertex set
        println(graph.vertices.filter { case (id, name::pos::others) => pos == "postdoc" }.count)

        // The equivalent for above command, but handle true, false seperatedly
        val newRDD = graph.vertices.filter {
            case (id, name::"postdoc"::others)  => true
            case _                              =>  false
        }
        val arr = newRDD.collect()

        println("=====")
        for (item <- arr) {
            println(item)
            println(item.getClass)
        }

    }
}
