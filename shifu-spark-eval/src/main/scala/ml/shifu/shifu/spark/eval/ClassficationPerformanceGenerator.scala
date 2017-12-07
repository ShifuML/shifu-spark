package ml.shifu.shifu.spark.eval

import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig, ColumnConfig}
import ml.shifu.shifu.util.{CommonUtils, Constants}

import org.apache.spark.{SparkContext, Accumulator}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.{ArrayBuffer, HashMap, Set, Map}
import scala.collection.{Iterator, Seq}

class ClassificationPerformanceGenerator(modelConfig : Broadcast[ModelConfig], evalConfig : Broadcast[EvalConfig], @transient context : SparkContext, accumMap : Map[String, Accumulator[Long]], modelNum : Int) extends Serializable {

    def genPerfByModels(scoreRDD : RDD[Map[String, Double]]) {
        scoreRDD.persist(StorageLevel.MEMORY_AND_DISK)
        
    }

    def genPerformance(key : String, socreRDD : RDD[Map[String, Double]]) {
        
    }

}
