package ml.shifu.shifu.spark.eval

import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig}
import ml.shifu.shifu.util.{CommonUtils, Constants}
import ml.shifu.shifu.spark.eval.exception._

import org.apache.spark.{SparkContext, Accumulator}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Set}
import scala.collection.{Iterator, Seq, Map}

class RegressionPerformanceGenerator(modelConfig : Broadcast[ModelConfig], evalConfig : Broadcast[EvalConfig], @transient context : SparkContext, accumMap : Map[String, Accumulator[Long]], modelNum : Int) extends Serializable {

    val scale = evalConfig.value.getScoreScale
    
    def genPerfByModels(scoreRDDRaw : RDD[scala.collection.mutable.Map[String, Any]]) {
        val scoreRDD = scoreRDDRaw.map(dataMap => dataMap.mapValues(_.toString.toDouble))
        scoreRDD.persist(StorageLevel.MEMORY_AND_DISK)

        for(num <- 0 until modelNum) {
            val key = "model" + num
            val sortedRDD = sortByFieldName(key, scoreRDD)
            genPerformance(sortedRDD, key)
        }
        val key = "avg"
        val sortedRDD = sortByFieldName(key, scoreRDD)
        genPerformance(sortedRDD, key)
        evalConfig.value.getScoreMetaColumns(modelConfig.value).asScala.map(x => {
            val rdd = sortByFieldName("meta_" + x, scoreRDD)
            genPerformance(rdd, "meta_" + x)
        })
    }

    def genPerformance(sortedRDD : RDD[Map[String, Double]], key : String) {
        //TODO: remove debug log
        Console.println("=================Start gen " + key + " perf ===========================")
        sortedRDD.persist(StorageLevel.MEMORY_AND_DISK)
        sortedRDD.saveAsTextFile("hdfs:///user/website/wzhu1/" + key + "-sorted.txt")
        val partitionsCount = sortedRDD.getNumPartitions
        val getLast = (iterator : Iterator[Map[String, Double]]) => {
            if(!iterator.hasNext) {
                Map[String, Double]()
            } else {
                var last = iterator.next
                while(iterator.hasNext) {
                    last = iterator.next
                }
                last
            }
        }

        val max = try {
            context.runJob(sortedRDD, getLast, Seq(partitionsCount - 1)).head.get(key) match {
                case Some(value) => value 
                case None => Console.println(key + " result is incorrect , check input")
                         throw new SparkEvalException("No max value of " + key, ExceptionInfo.NoScoreResult)
                }
        } catch {
            case SparkEvalException(msg, exInfo) => { 
                Console.println(exInfo.toString + " " + msg)
                return
            }
        }
        val min = sortedRDD.first.getOrElse(key, scale.toDouble)
        val numBucket = evalConfig.value.getPerformanceBucketNum
        val step = 1d / numBucket
        val sumOfPartitionsData = sortedRDD.mapPartitionsWithIndex((index : Int, iterator : Iterator[Map[String, Double]]) => {
            var tagSum = 0d
            var weightTagSum = 0d
            var recordCountSum = 0d
            var weightSum = 0d
            while(iterator.hasNext) {
                val current = iterator.next
                tagSum += current.getOrElse("tag", 0d)
                weightTagSum += current.getOrElse("weightTag", 0d)
                weightSum += current.getOrElse("weightColumn", 0d)
                recordCountSum += 1
            }
            Array(Map(("record" + index, recordCountSum), ("tagSum" + index, tagSum), ("weightSum" + index, weightSum), ("weightTagSum" + index, weightTagSum))).iterator
        }).collect().reduce((map1, map2) => map1 ++ map2)
        Console.println(sumOfPartitionsData)
        val recordCount = accumMap.get(Constants.COUNTER_RECORDS).map(accum => accum.value).getOrElse(0l)
        val posCount = accumMap.get(Constants.COUNTER_POSTAGS).map(accum => accum.value).getOrElse(0l)
        val weightPosSum = accumMap.get(Constants.COUNTER_WPOSTAGS).map(accum => accum.value).getOrElse(0l)
        val negCount = accumMap.get(Constants.COUNTER_NEGTAGS).map(accum => accum.value).getOrElse(0l)
        val weightNegSum = accumMap.get(Constants.COUNTER_WNEGTAGS).map(accum => accum.value).getOrElse(0l)
        val sumOfPartitions = context.broadcast(sumOfPartitionsData)
        val perfResultRDD = sortedRDD.mapPartitionsWithIndex((index : Int, iterator : Iterator[Map[String, Double]]) => {
            val weightTotalSum = weightPosSum + weightNegSum
            var tagBase = 0d
            var weightTagBase = 0d
            var recordBase = 0d
            var weightBase = 0d
            //TODO: need code refact, it is too java 
            for(i <- 0 until index) {
                tagBase += sumOfPartitions.value.getOrElse("tagSum" + i, 0d)
                weightTagBase += sumOfPartitions.value.getOrElse("weightTagSum" + i, 0d)
                recordBase += sumOfPartitions.value.getOrElse("record" + i, 0d)
                weightBase += sumOfPartitions.value.getOrElse("weightSum" + i, 0d)
            }
            var currentTag = tagBase
            var currentWeightTag = weightTagBase
            var currentRecord = recordBase
            var currentWeight = weightBase
            val resultArray = new ArrayBuffer[Map[String, Double]]

            while(iterator.hasNext) {
                val map = iterator.next
                currentRecord += 1
                currentTag += map.getOrElse("tag", 0d)
                currentWeightTag += map.getOrElse("weightTag", 0d)

                val score = map.getOrElse(key, 0d)
                val tp = posCount - currentTag
                val tn = currentRecord - currentTag
                val fn = currentTag
                val fp = negCount - tn
                val weightTp = weightPosSum - currentWeightTag
                val weightTn = currentWeightTag - currentWeight
                val weightFn = currentWeightTag
                val weightFp = weightNegSum - weightTn
                val actionRate = (tp + fp) / recordCount.toDouble
                val weightActionRate = (weightTp + weightFp) / weightTotalSum.toDouble
                val recall = tp / (tp + fn).toDouble
                val weightRecall = weightTp / (weightTp + weightFn).toDouble
                val precision = tp / (tp + fp).toDouble
                val weightPrecision = weightTp / (weightTp + weightFp).toDouble
                val fpr = fp / (fp + tn).toDouble
                val weightFpr = weightFp / (weightFp + weightTn).toDouble

                resultArray += Map(("actionRate", actionRate), ("weightActionRate", weightActionRate), ("recall", recall), ("weightRecall", weightRecall),
                    ("precision", precision), ("weightPrecision", weightPrecision), ("fpr", fpr), ("weightFpr", weightFpr), ("score", score), ("currentRecord", currentRecord))
            }
            val result = resultArray.foldLeft(ArrayBuffer(resultArray.head))((array : ArrayBuffer[Map[String, Double]], x : Map[String, Double]) => {
                var keep = false
                val lIterator = x.toIterator
                while(lIterator.hasNext && !keep) {
                    val (k, v) = lIterator.next
                    k match {
                        case "score" => keep = v.toInt - array.last.getOrElse(k, 0d).toInt > 0
                        case _ => keep = (v * numBucket).toInt - (array.last.getOrElse(k, 0d) *  numBucket).toInt > 0
                    }
                }
                if(keep) {
                    array += x
                }
                array
            })
            result.iterator
        })
        //TODO: change hard code path
        val perfResult = perfResultRDD.saveAsTextFile("hdfs:///user/website/wzhu1/" + key + "-perf.txt")
        sortedRDD.unpersist(true)
    }
    
    def sortByFieldName(name : String, scoreRDD : RDD[scala.collection.Map[String, Double]]) : RDD[Map[String, Double]] = {
        scoreRDD.map(map => map.filterKeys(key => Set(name, "weightTag", "tag", "weightColumn").contains(key))).sortBy(map => map.getOrElse(name, Double.NaN))
    }
}
