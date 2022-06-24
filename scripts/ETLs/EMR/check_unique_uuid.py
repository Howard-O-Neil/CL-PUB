import spark.implicits._

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.control._
import org.apache.spark.sql.{functions => sparkf}
import org.apache.spark.{sql => sparkSQL}
import org.apache.spark.sql.{types => sparkSQL_T}


org_vertex_dir  = "s3://recsys-bucket/data_lake/arnet/tables/org_vertex/merge-0"
org_edge_dir    = "s3://recsys-bucket/data_lake/arnet/tables/org_edge/merge-0"

spark.read.parquet(org_vertex_dir).createOrReplaceTempView("temp")
spark.sql("""
    select count(_id)
    from temp
    group by _id
    having count(_id) > 1
""").show()

spark.read.parquet(org_edge_dir).createOrReplaceTempView("temp")
spark.sql("""
    select count(_id)
    from temp
    group by _id
    having count(_id) > 1
""").show()