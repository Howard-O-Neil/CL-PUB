package graphx
import java.io._

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

object App_params {
    var neighbor_map_in     = collection.mutable.Map[Long, List[Long]]()
    var neighbor_map_out    = collection.mutable.Map[Long, List[Long]]()
    var ranking_map         = collection.mutable.Map[Long, Float]()
    var converge_map        = collection.mutable.Map[Long, Boolean]()
    var neighborProb_map    = collection.mutable.Map[Long, Float]()
    var restartProb_map     = collection.mutable.Map[Long, Float]()

    val converge        = 0.0001f
    var is_converge     = false
    var restart         = false

}

object App {
    val params      = App_params
    var listProb    = List[Float]()
    var maxProb     = 20
    val probDist    = 0.75f
    var generator   = new Random()

    val spark = SparkSession.builder
        .config("spark.app.name", "Recsys")
        .config("spark.master", "local[*]")
        .config("spark.submit.deployMode", "client")
        .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar")
        .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
        .getOrCreate()

    def writeToFile(message: String) = {
        val out: BufferedWriter = new BufferedWriter(
            new FileWriter("/recsys/prototype/graph_x_RWR/output.txt", true));

        out.write(message)
        out.close()
    }

    def initListProb() = {
        var currentProb = 0f

        val loop = new Breaks
        loop.breakable {
            while (true) {
                if (currentProb >= maxProb)
                    loop.break
                
                listProb = listProb ::: List(currentProb)
                currentProb += probDist
            }
        }
    }

    def randNeighborProb() = {
        val schema      = sparkt.StructType(Array(
                            sparkt.StructField("id", sparkt.IntegerType, false)))
        val seedData    = new java.util.ArrayList[sparkSQL.Row]()
        for (i <- 1 to params.neighborProb_map.size)
            seedData.add(sparkSQL.Row(i))

        val dfr                 = spark.createDataFrame(seedData, schema)
        val randomValues_df     = dfr.select("id")
                .withColumn("uniform", sparkf.rand(System.nanoTime()))
                .withColumn("normal", sparkf.randn(System.nanoTime()))
        val randomValues = randomValues_df.collect().map(
            item => item(1).asInstanceOf[Double].toFloat)
        
        // var randomValues = List[Float]()
        // for (i <- 1 to params.neighborProb_map.size)
        //     randomValues = randomValues ::: List(generator.nextInt(maxProb) / maxProb.toFloat)

        var idx = 0
        params.neighborProb_map.foreach {
            case (id, prob) => {
                params.neighborProb_map(id) = randomValues(idx)
                idx += 1
            }
        }
    }

    def randResetProb() = {
        val schema      = sparkt.StructType(Array(
                            sparkt.StructField("id", sparkt.IntegerType, false)))
        val seedData    = new java.util.ArrayList[sparkSQL.Row]()
        for (i <- 1 to params.restartProb_map.size)
            seedData.add(sparkSQL.Row(i))

        val dfr                 = spark.createDataFrame(seedData, schema)
        val randomValues_df     = dfr.select("id")
                .withColumn("uniform", sparkf.rand(System.nanoTime()))
                .withColumn("normal", sparkf.randn(System.nanoTime()))
        val randomValues = randomValues_df.collect().map(
            item => item(1).asInstanceOf[Double].toFloat)

        // var randomValues = List[Float]()
        // for (i <- 1 to params.restartProb_map.size)
        //     randomValues = randomValues ::: List(generator.nextInt(maxProb) / maxProb.toFloat)
        
        var idx = 0
        params.restartProb_map.foreach {
            case (id, prob) => {
                params.restartProb_map(id) = randomValues(idx)
                idx += 1
            }
        }
    }


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

        val dampingFactor   = 0.85f
        val restartProb     = 0.15f
        val total_vertices = graph.vertices.count().toFloat

        val connected_groups_t = graph.ops.connectedComponents().vertices
        var connected_groups = connected_groups_t.map(x => (x._2, List(x._1)))

        connected_groups = connected_groups.reduceByKey(
            (a, b) => a ::: b
        )

        for (group <- connected_groups.collect()) {
            val subGraph            = graph.subgraph(vpred = (vid, attr) => group._2.contains(vid))
            val subGraphVertices    = subGraph.vertices.count()

            // Build a quick search map

            params.neighbor_map_in     = collection.mutable.Map[Long, List[Long]]()
            params.neighbor_map_out    = collection.mutable.Map[Long, List[Long]]()
            params.ranking_map         = collection.mutable.Map[Long, Float]()
            params.converge_map        = collection.mutable.Map[Long, Boolean]()
            params.neighborProb_map    = collection.mutable.Map[Long, Float]()
            params.restartProb_map     = collection.mutable.Map[Long, Float]()

            for (neighbor <- subGraph.ops.collectNeighborIds(EdgeDirection.Out).collect()) {
                params.neighbor_map_out    +=  (neighbor._1 -> neighbor._2.toList)
                params.ranking_map         +=  (neighbor._1 -> 1f / total_vertices)
                params.converge_map        +=  (neighbor._1 -> false)
                params.neighborProb_map    +=  (neighbor._1 -> 0f)
                params.restartProb_map     +=  (neighbor._1 -> 0f)
            }
            
            for (neighbor <- subGraph.ops.collectNeighborIds(EdgeDirection.In).collect()) {
                params.neighbor_map_in     += (neighbor._1 -> neighbor._2.toList)
            }

            randNeighborProb()
            randResetProb()

            var g: Graph[(String, Float, Int, Float), Double] = subGraph.mapVertices((id, attr) =>
                (attr._1, params.neighborProb_map(id), 0, params.restartProb_map(id)))

            // Experimental
            // the idea is to somehow precalculate maxStep of walker 
            //          instead of run until converge
            // but i have not yet find the formula yet
            val maxStep         = subGraph.vertices.count() * 5
            var step            = 0
            params.is_converge  = false

            val loop = new Breaks
            loop.breakable {
                while (true) {
                    val msgs        = g.aggregateMessages[Int] (
                        triplet => {
                            var converge_count = 0
                            params.converge_map foreach {
                                case (key, value) => if (value) converge_count += 1
                            }

                            if (converge_count < subGraphVertices) {
                                if (!params.restart) {
                                    if (triplet.srcAttr._4 <= restartProb) {
                                        params.restart = true
                                    }
                                }

                                if (!params.restart && triplet.srcAttr._2 <= dampingFactor) {
                                    var ranking = 0f
                                    for (neighbor <- params.neighbor_map_in(triplet.srcId)) {
                                        val outlinks = params.neighbor_map_out(neighbor)
                                        
                                        if (outlinks.length <= 1) {
                                            ranking += params.ranking_map(neighbor) / total_vertices
                                        } else {
                                            ranking += params.ranking_map(neighbor) / outlinks.length
                                        }
                                    }

                                    if (ranking == 0f) {
                                        ranking = params.ranking_map(triplet.srcId)
                                    } else {
                                        ranking = (dampingFactor * ranking) + ((1f - dampingFactor) / total_vertices)
                                    }

                                    if (!params.converge_map(triplet.srcId) &&
                                        (params.ranking_map(triplet.srcId) - ranking).abs > params.converge) {
                                        params.ranking_map(triplet.srcId)  = ranking
                                    } else {
                                        params.converge_map(triplet.srcId) = true
                                    }
                                    triplet.sendToSrc(triplet.srcAttr._3 + 1)
                                }
                            } else {
                                params.is_converge = true
                            }
                        },
                        (a, b) => a + b
                    )
                    params.restart = false

                    if (params.is_converge || subGraphVertices <= 1) {
                        loop.break
                    }

                    g = g.ops.joinVertices(msgs) (
                        (id, oldAttr, newDist) =>
                            (oldAttr._1, oldAttr._2, newDist, oldAttr._4)
                    )

                    randNeighborProb()
                    randResetProb()
                    g = g.mapVertices((id, attr) =>
                        (attr._1, params.neighborProb_map(id), attr._3, params.restartProb_map(id)))

                    g.vertices.collect()
                }
            }
            writeToFile(params.ranking_map.mkString("\n") + "\n\n")
        }
    }
}