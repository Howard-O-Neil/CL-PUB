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
import org.apache.spark.storage._

object App {
    val citation_vertex_dir     = "gs://clpub/data_lake/arnet/tables/citation_vertex/merge-0"
    val citation_edge_dir       = "gs://clpub/data_lake/arnet/tables/citation_edge/merge-0"
    val dst_dir                 = "gs://clpub/data_lake/arnet/tables/citation_rwr_bias/iter-"

    def main(args: Array[String]) = {
        var spark: SparkSession     = (SparkSession.builder.getOrCreate())
        var sc: SparkContext        = spark.sparkContext
        val optimized_partition     = 800

        val vertex_local_write  = spark.read.parquet(citation_vertex_dir).repartition(optimized_partition)
        val edge_local_write    = spark.read.parquet(citation_edge_dir).repartition(optimized_partition)

        vertex_local_write.write.mode("overwrite").parquet("hdfs:///recsys/temp/citation_vertex")
        edge_local_write.write.mode("overwrite").parquet("hdfs:///recsys/temp/citation_edge")

        val vertex_rdd = spark.read.parquet("hdfs:///recsys/temp/citation_vertex").repartition(optimized_partition).rdd.map(
            x => (x(4).asInstanceOf[Number].longValue, 0D)
        )
        val edge_rdd = spark.read.parquet("hdfs:///recsys/temp/citation_edge").repartition(optimized_partition).rdd.map(
            x => Edge(x(5).asInstanceOf[Number].longValue,
                    x(6).asInstanceOf[Number].longValue,
                    x(7).asInstanceOf[Number].floatValue)
        )

        val num_vertex  = vertex_rdd.count()
        val num_edge    = edge_rdd.count()
        println(s"[===== Vertex count   = ${num_vertex} =====]")
        println(s"[===== Edge count     = ${num_edge} =====]")

        val default_rank        = 1f
        val default_restart_vec = 1f / num_vertex
        val resProb             = 0.15f
        val damFac              = 1f - resProb
        val maxStep             = 15

        val graph = Graph(vertex_rdd, edge_rdd) 

        var g = graph.partitionBy(
            PartitionStrategy.EdgePartition2D).mapVertices((id, attr) => (default_rank, 0, 0, 0f))

        var sum_weight = graph.mapVertices((id, attr) => 0f)
        val sum_w_msgs = sum_weight.aggregateMessages[Float] (
            triplet => {
                triplet.sendToSrc(triplet.attr.floatValue)
            },
            (a, b) => a + b
        )
        sum_weight = sum_weight.ops.joinVertices(sum_w_msgs) {
            (id, oldAttr, newAttr) => oldAttr + newAttr
        }

        g = g.outerJoinVertices(graph.inDegrees) {
            (vid, vdata, deg) => (vdata._1, deg.getOrElse(0), vdata._3, vdata._4)
        }
        g = g.outerJoinVertices(graph.outDegrees) {
            (vid, vdata, deg) => (vdata._1, vdata._2, deg.getOrElse(0), vdata._4)
        }
        g = g.outerJoinVertices(sum_weight.vertices) {
            (vid, vdata, sw) => (vdata._1, vdata._2, vdata._3, sw.getOrElse(0f))
        }

        g = g.mapVertices((id, attr) => {
            if (attr._2 == 0 || attr._3 == 0) {
                ((resProb * default_restart_vec), attr._2, attr._3, attr._4)
            } else (attr._1, attr._2, attr._3, attr._4)
        })

        var step = 0
        var loop = new Breaks
        loop.breakable {
            while (true) {
                if (step >= maxStep) {
                    loop.break
                }
                println(s"[===== Iteration ${step} ... =====]")

                val msgs = g.aggregateMessages[Float](
                    triplet => {
                        val bias_ranking = (triplet.srcAttr._1 / triplet.srcAttr._3) *
                            (triplet.attr.floatValue / triplet.srcAttr._4)
                        triplet.sendToDst(bias_ranking)
                    },
                    (a, b) => a + b
                )

                g = g.ops.joinVertices(msgs) {
                    (id, oldAttr, newAttr) => {
                        ((resProb * default_restart_vec) + (damFac * newAttr), oldAttr._2, oldAttr._3, oldAttr._4)
                    }
                }

                val rwr_df = spark.createDataFrame(g.vertices)
                rwr_df.select(sparkf.col("_1").alias("id"), sparkf.col("_2").alias("node")).write.mode(
                    "overwrite").parquet(dst_dir + step.toString)
            
                step += 1
            }
        }
    }
}

val app = App
app.main(Array("main"))
