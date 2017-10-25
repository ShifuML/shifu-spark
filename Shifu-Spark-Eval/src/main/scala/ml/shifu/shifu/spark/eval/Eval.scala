package ml.shifu.shifu.spark.eval

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

trait Eval {

    def evalScore(inputRDD : RDD[String]) : RDD[Map[String, Double]]
}
