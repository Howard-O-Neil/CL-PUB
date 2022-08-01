import spark.implicits._

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.{functions => sparkf}
import org.apache.spark.{sql => sparkSQL}
import org.apache.spark.sql.{types => sparkSQL_T}

import scala.util.control._
import scala.collection.mutable._

var test_str = "1;2,3;4,5;7,8,9;100"
test_str = "1"
def add_point(point: Int) = {
    val list_split = test_str.split(";").toList
    list_split.map(x => x + "," + point.toString).reduce((x, y) => x + ";" + y)
}

add_point(15)