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
    val org_vertex_dir          = "gs://clpub/data_lake/arnet/tables/org_vertex/merge-0"
    val org_edge_dir            = "gs://clpub/data_lake/arnet/tables/org_edge/merge-0"
    val org_group_dir           = "gs://clpub/data_lake/arnet/tables/org_group/merge-0"
    val dst_dir                 = "gs://clpub/data_lake/arnet/tables/org_discrete_algo/merge-0"

    def main(args: Array[String]) = {
        var spark: SparkSession     = (SparkSession.builder.getOrCreate())
        var sc: SparkContext        = spark.sparkContext
        val optimized_partition     = 500

        val vertex_local_write  = spark.read.parquet(org_vertex_dir).repartition(optimized_partition)
        val edge_local_write    = spark.read.parquet(org_edge_dir).repartition(optimized_partition)
        val group_local_write   = spark.read.parquet(org_group_dir).repartition(optimized_partition)

        vertex_local_write.write.mode("overwrite").parquet("hdfs:///recsys/temp/org_vertex")
        edge_local_write.write.mode("overwrite").parquet("hdfs:///recsys/temp/org_edge")
        group_local_write.write.mode("overwrite").parquet("hdfs:///recsys/temp/org_group")

        val vertex_rdd = spark.read.parquet("hdfs:///recsys/temp/org_vertex").repartition(optimized_partition).rdd.map(
            x => (x(4).asInstanceOf[Number].longValue, 0D)
        )
        val edge_rdd = spark.read.parquet("hdfs:///recsys/temp/org_edge").repartition(optimized_partition).rdd.map(
            x => Edge(x(5).asInstanceOf[Number].longValue,
                    x(6).asInstanceOf[Number].longValue,
                    x(7).asInstanceOf[Number].floatValue)
        )

        val group_df = spark.read.parquet("hdfs:///recsys/temp/org_group")
        group_df.createOrReplaceTempView("group_df")

        val max_group_id = spark.sql("select max(group) as max_group from group_df").collect()(0)(0).asInstanceOf[Number].floatValue
        val min_group_id = spark.sql("select min(group) as min_group from group_df").collect()(0)(0).asInstanceOf[Number].floatValue

        val group_rdd = group_df.repartition(optimized_partition).rdd.map(
            x => (x(1).asInstanceOf[Number].longValue, (x(0).asInstanceOf[Number].longValue, x(2).asInstanceOf[Number].longValue))
        )

        val num_vertex  = vertex_rdd.count()
        val num_edge    = edge_rdd.count()
        println(s"[===== Vertex count   = ${num_vertex} =====]")
        println(s"[===== Edge count     = ${num_edge} =====]")

        val default_rank        = 1f
        val resProb             = 0.15f
        val damFac              = 1f - resProb
        val maxStep             = 15

        val graph = Graph(vertex_rdd, edge_rdd) 

        var g = graph.partitionBy(
            PartitionStrategy.EdgePartition2D).mapVertices((id, attr) => (default_rank, 0, 0, 0L, 0f))

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
            (vid, vdata, deg) => (vdata._1, deg.getOrElse(0), vdata._3, vdata._4, vdata._5)
        }
        g = g.outerJoinVertices(graph.outDegrees) {
            (vid, vdata, deg) => (vdata._1, vdata._2, deg.getOrElse(0), vdata._4, vdata._5)
        }
        g = g.outerJoinVertices(group_rdd) {
            (vid, vdata, group_id) => (vdata._1, vdata._2, vdata._3, group_id.getOrElse((0L, 0L))._2 + 100L, vdata._5)
        }
        g = g.outerJoinVertices(sum_weight.vertices) {
            (vid, vdata, sw) => (vdata._1, vdata._2, vdata._3, vdata._4, sw.getOrElse(0f))
        }

        g = g.mapVertices((id, attr) => {
            val restart_vect = 1f / (math.log(attr._4.doubleValue)).floatValue
            ((resProb * restart_vect), attr._2, attr._3, attr._4, attr._5)
        })

        val rwr_df = spark.createDataFrame(g.vertices)
        rwr_df.select(sparkf.col("_1").alias("id"), sparkf.col("_2").alias("node")).write.mode(
            "overwrite").parquet(dst_dir)
    }
}

val app = App
app.main(Array("main"))
