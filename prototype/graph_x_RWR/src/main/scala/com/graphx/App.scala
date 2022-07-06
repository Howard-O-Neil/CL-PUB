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
    var neighbor_map_out    = collection.mutable.Map[Long, List[Long]]()
    var ranking_map         = collection.mutable.Map[Long, Float]()
    var neighborProb_map    = collection.mutable.Map[Long, Float]()
    var restartProb_map     = collection.mutable.Map[Long, Float]()

    var restart         = false

    // RWR max Step concept
    // I haven't not yet found good formula on calculating MaxStep or MaxSuperStep
    var maxStep         = 10
    var step            = 0
}

object App {
    val params      = App_params

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

    def randNeighborProb() = {
        val schema      = sparkt.StructType(Array(
                            sparkt.StructField("id", sparkt.IntegerType, false),
                            sparkt.StructField("draft", sparkt.StringType, false)))
        var seedData: Seq[(Int, String)] = Seq()

        for (i <- 1 to params.restartProb_map.size)
            seedData = seedData :+ (i, "")

        val rdd = spark.sparkContext.parallelize(seedData)
        val max_rand    = 10f
        val dfr                 = spark.createDataFrame(rdd.map(attr => sparkSQL.Row(attr._1, attr._2)), schema)
        val randomValues_df     = dfr.select("id")
                // .withColumn("uniform", sparkf.rand(System.nanoTime()))
                .withColumn("uniform", sparkf.floor(sparkf.rand(System.nanoTime()) * max_rand) / max_rand)
                .withColumn("normal", sparkf.randn(System.nanoTime()))
        val randomValues = randomValues_df.collect().map(
            item => item(1).asInstanceOf[Double].toFloat)

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
                            sparkt.StructField("id", sparkt.IntegerType, false),
                            sparkt.StructField("draft", sparkt.StringType, false)))
        var seedData: Seq[(Int, String)] = Seq()

        for (i <- 1 to params.restartProb_map.size)
            seedData = seedData :+ (i, "")

        val rdd = spark.sparkContext.parallelize(seedData)
        val max_rand    = 10000f
        val dfr                 = spark.createDataFrame(rdd.map(attr => sparkSQL.Row(attr._1, attr._2)), schema)
        val randomValues_df     = dfr.select("id")
                // .withColumn("uniform", sparkf.rand(System.nanoTime()))
                .withColumn("uniform", sparkf.floor(sparkf.rand(System.nanoTime()) * max_rand) / max_rand)
                .withColumn("normal", sparkf.randn(System.nanoTime()))
        val randomValues = randomValues_df.collect().map(
            item => item(1).asInstanceOf[Double].toFloat)
        
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
                Edge(17L, 1L, 9D), Edge(1L, 17L, 9D)))

        val graph = Graph(users, relationships)

        val dampingFactor   = 0.85f
        val restartProb     = 0.15f
        val total_vertices  = graph.vertices.count().toFloat

        val connected_groups_t = graph.ops.connectedComponents().vertices
        var connected_groups = connected_groups_t.map(x => (x._2, List(x._1)))

        connected_groups = connected_groups.reduceByKey(
            (a, b) => a ::: b
        )

        // writeToFile(graph.inDegrees.collect().mkString("\n"))

        // sys.exit

        var result_ranking_map      = collection.immutable.Seq[(Long, Float)]()
        var single_node_count       = 0
        for (group <- connected_groups.collect()) {
            val subGraph            = graph.subgraph(vpred = (vid, attr) => group._2.contains(vid))
            val subGraphVertices    = subGraph.vertices.count()

            if (subGraphVertices == 1)
                single_node_count += 1
        }

        for (group <- connected_groups.collect()) {
            val subGraph            = graph.subgraph(vpred = (vid, attr) => group._2.contains(vid))
            val subGraphVertices    = subGraph.vertices.count()
            val defaultRank         = 1f
            val defaultRestartVec   = 1f / total_vertices.toFloat
            // Build a quick search map

            params.neighbor_map_out    = collection.mutable.Map[Long, List[Long]]()
            params.ranking_map         = collection.mutable.Map[Long, Float]()
            params.neighborProb_map    = collection.mutable.Map[Long, Float]()
            params.restartProb_map     = collection.mutable.Map[Long, Float]()

            for (neighbor <- subGraph.ops.collectNeighborIds(EdgeDirection.Out).collect()) {
                params.neighbor_map_out     +=  (neighbor._1 -> neighbor._2.toList)
                params.neighborProb_map     +=  (neighbor._1 -> 0f)
                params.restartProb_map      +=  (neighbor._1 -> 0f)

                if (neighbor._2.toList.length <= 0) {                        
                    params.ranking_map      +=  (neighbor._1 -> defaultRestartVec)
                } else {
                    params.ranking_map      +=  (neighbor._1 -> 0f)
                }
            }

            randNeighborProb()
            randResetProb()

            var g: Graph[(String, Float, Float, Float, Int), Double] = subGraph.mapVertices((id, attr) =>
                (attr._1, params.neighborProb_map(id), defaultRank, params.restartProb_map(id), 0))

            params.restart      = false
            params.step         = 0

            val loop = new Breaks
            loop.breakable {
                while (true) {
                    val msgs        = g.aggregateMessages[Float] (
                        triplet => {
                            var outlinks = params.neighbor_map_out(triplet.srcId)
                            triplet.sendToDst(triplet.srcAttr._3 / outlinks.length)

                            // if (!params.restart && triplet.srcAttr._4 <= restartProb)
                            //     params.restart = true

                            // if (triplet.dstAttr._5 < 3 || (!params.restart && triplet.srcAttr._2 <= dampingFactor)) {
                            //     var outlinks = params.neighbor_map_out(triplet.srcId)
                            //     triplet.sendToDst(triplet.srcAttr._3 / outlinks.length)
                            // }
                        },
                        (a, b) => a + b
                    )

                    randNeighborProb()
                    randResetProb()

                    val newVertices = g.vertices.join(msgs).map(
                        x => {
                            val id              = x._1
                            val txt             = x._2._1._1
                            val neighbor_prop   = x._2._1._2
                            val oldDist         = x._2._1._3
                            val restart_prop    = x._2._1._4
                            val order           = x._2._1._5
                            var newDist         = x._2._2
                            
                            var ranking         = newDist + (single_node_count * (defaultRank / total_vertices))
                            ranking             = (restartProb * defaultRestartVec) + (dampingFactor * ranking)
                            params.ranking_map(id)      = ranking
                            (id, (txt, params.neighborProb_map(id), ranking, params.restartProb_map(id), order + 1))
                        })

                    val mergeVertices = g.vertices.union(newVertices).reduceByKey(
                        (a, b) => if (a._5 >= b._5) a else b
                    )

                    g = g.ops.joinVertices(mergeVertices) {
                        (id, oldAttr, newAttr) => newAttr
                    }

                    params.restart = false
                    params.step += 1
                    if (subGraphVertices <= 1 || params.step >= params.maxStep) {
                        loop.break
                    }

                    // For terminal logging while waiting
                    g.vertices.take(5)
                }
            }
            result_ranking_map = result_ranking_map ++ params.ranking_map.toSeq
        }
        writeToFile(result_ranking_map.sortBy(_._2) .mkString("\n") + "\n\n")
        writeToFile("===== Compare to Standard PageRank \n\n")
        writeToFile(graph.pageRank(0.01f).vertices.collect().sortBy(_._2)
            .mkString("\n") + "\n\n")
    }
}