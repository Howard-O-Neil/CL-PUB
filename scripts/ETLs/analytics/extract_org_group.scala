import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.{functions => sparkf}
import org.apache.spark.{sql => sparkSQL}
import org.apache.spark.sql.{types => sparkSQL_T}
import org.apache.spark.TaskContext

import scala.util.control._
import scala.collection.mutable._

val org_vertex_dir  = "s3://recsys-bucket/data_lake/arnet/tables/org_vertex/merge-0"
val org_edge_dir    = "s3://recsys-bucket/data_lake/arnet/tables/org_edge/merge-0"
val dst_dir         = "s3://recsys-bucket/data_lake/arnet/tables/org_group/merge-0"

var graph : Graph[Double, Double]   = null

val spark   = SparkSession.builder.getOrCreate()
val sc      = spark.sparkContext

val vertex_rdd = spark.read.parquet(org_vertex_dir).rdd.map(
    x => (x(4).asInstanceOf[Number].longValue, 0D)
)
val edge_rdd = spark.read.parquet(org_edge_dir).rdd.map(
    x => (Edge(
        x(5).asInstanceOf[Number].longValue,
        x(6).asInstanceOf[Number].longValue,
        x(7).asInstanceOf[Number].doubleValue
)))
val graph = Graph(vertex_rdd, edge_rdd)

val connected_groups_t  = graph.ops.connectedComponents().vertices
val connected_groups = connected_groups_t.map(x => (x._2, x._1))

val group_df = spark.createDataFrame(connected_groups)
val new_group = group_df.select(sparkf.col("_1").alias("group"), sparkf.col("_2").alias("item"))
new_group.createOrReplaceTempView("new_group")

val new_group_df = spark.sql("""
    select ng.group, ng.item, group_count.num_item
    from new_group as ng
        inner join (
            select ng2.group as group, count(item) as num_item
            from new_group as ng2
            group by ng2.group
        ) as group_count on group_count.group = ng.group
""")

new_group_df.write.mode("overwrite").parquet(dst_dir)
