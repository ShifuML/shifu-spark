package ml.shifu.shifu.spark.eval

import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig}
import ml.shifu.shifu.util.{CommonUtils, Constants}
import ml.shifu.shifu.spark.eval.exception._

import org.apache.spark.{SparkContext, Accumulator}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Set, HashSet}
import scala.collection.{Iterator, Seq, Map}

class RegressionPerformanceGenerator(modelConfig : Broadcast[ModelConfig], evalConfig : Broadcast[EvalConfig], @transient context : SparkContext, accumMap : Map[String, Accumulator[Long]], modelNum : Int) extends Serializable {

    val scale = evalConfig.value.getScoreScale
    
    def genPerfByModels(scoreRDDRaw : RDD[scala.collection.mutable.Map[String, Any]]) {
        val posTag = new HashSet[String]
        posTag ++= modelConfig.value.getPosTags.asScala
        val negTag = new HashSet[String]
        negTag ++= modelConfig.value.getNegTags.asScala
        
        //val scoreRDD = scoreRDDRaw.map(dataMap => dataMap.mapValues(_.toString.toDouble))
        val scoreRDD = scoreRDDRaw.filter(dataMap => { 
                posTag.contains(dataMap.getOrElse("tag", "No Tag Data").toString) || 
                    negTag.contains(dataMap.getOrElse("tag", "No Tag Data").toString)
            }
        ).map(dataMap => 
        dataMap.updated("tag", 
            posTag.contains(dataMap.getOrElse("tag", "No Tag Data").toString) match {
                case true => 1d
                case _ => 0d
        }).mapValues(_.toString.toDouble))

        scoreRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val map = new HashMap[String, Array[Map[String, Double]]]
        for(num <- 0 until modelNum) {
            val key = "model" + num
            val sortedRDD = sortByFieldName(key, scoreRDD)
            val perfArray = genPerformance(sortedRDD, key)
            map.put(key, perfArray)
            /*
            val t = new Thread(new Runnable(){
                def run() {
                    GainChart.draw(perfArray, key + "_GainChart.html", modelConfig.value, evalConfig.value)
                }
            })
            t.start
            */
        }
        val key = "avg"
        val sortedRDD = sortByFieldName(key, scoreRDD)
        val avgPerfData = genPerformance(sortedRDD, key)
        if(modelNum > 1) {
            map.put(key, avgPerfData)
        }
        evalConfig.value.getScoreMetaColumns(modelConfig.value).asScala.map(x => {
            val rdd = sortByFieldName("meta_" + x, scoreRDD)
            val perfArray = genPerformance(rdd, "meta_" + x)
            map.put(x, perfArray)
        })
        //for draw chart scale, the Map[String, Double] score value should be scale down only one time
       // val perfArray : Array[(String, Array[Map[String, Double]])] = map.mapValues(x => x.map(y => y.updated("score", y.getOrElse("score", 0d)))).toArray
        val perfArray : Array[(String, Array[Map[String, Double]])] = map.toArray
        GainChart.draw(perfArray, "GainChart.html", modelConfig.value, evalConfig.value)
        PrAndRocChart.draw(perfArray, "PrAndRocChart.html", modelConfig.value, evalConfig.value)
    }

    def genPerformance(sortedRDD : RDD[Map[String, Double]], key : String) = {
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
            case SparkEvalException(msg, exInfo, null) => { 
                Console.println(exInfo.toString + " " + msg)
            }
        }
        val min = sortedRDD.first.getOrElse(key, scale.toString.toDouble)
        val numBucket = evalConfig.value.getPerformanceBucketNum
        val sumOfPartitionsData = sortedRDD.mapPartitionsWithIndex((index : Int, iterator : Iterator[Map[String, Double]]) => {
            var tagSum = 0d
            var weightTagSum = 0d
            var recordCountSum = 0d
            var weightSum = 0d
            while(iterator.hasNext) {
                val current = iterator.next
                tagSum += current.getOrElse("tag", 0d).toString.toDouble
                weightTagSum += current.getOrElse("weightTag", 0d).toString.toDouble
                weightSum += current.getOrElse("weightColumn", 0d).toString.toDouble
                recordCountSum += 1
            }
            Array(Map(("record" + index, recordCountSum), ("tagSum" + index, tagSum), ("weightSum" + index, weightSum), ("weightTagSum" + index, weightTagSum))).iterator
        }).collect().reduce((map1, map2) => map1 ++ map2)

        val recordCount = accumMap.get(Constants.COUNTER_RECORDS).map(accum => accum.value).getOrElse(0l)
        val posCount = accumMap.get(Constants.COUNTER_POSTAGS).map(accum => accum.value).getOrElse(0l)
        val weightPosSum = accumMap.get(Constants.COUNTER_WPOSTAGS).map(accum => accum.value).getOrElse(0l)
        val negCount = accumMap.get(Constants.COUNTER_NEGTAGS).map(accum => accum.value).getOrElse(0l)
        val weightNegSum = accumMap.get(Constants.COUNTER_WNEGTAGS).map(accum => accum.value).getOrElse(0l)
        
        Console.println("recordCount is " + recordCount)
        Console.println("posCount is " + posCount)
        Console.println("weightPosSum is " + weightPosSum)
        Console.println("negCount is " + negCount)
        Console.println("weightNegSum is " + weightNegSum)

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

                val score = map.getOrElse(key, 0d)
                val tp = posCount - currentTag
                val tn = currentRecord - currentTag
                val fn = currentTag
                val fp = negCount - tn
                val weightTp = weightPosSum - currentWeightTag
                val weightTn = currentWeight - currentWeightTag
                val weightFn = currentWeightTag
                val weightFp = weightNegSum - weightTn
                val actionRate = (tp + fp) / recordCount.toDouble
                val weightActionRate = (weightTp + weightFp) / weightTotalSum.toDouble
                val recall = tp / (tp + fn).toDouble
                val weightRecall = weightTp / (weightTp + weightFn).toDouble

                val precision = if(tp + fp == 0d){
                    1d
                } else {
                    tp / (tp + fp).toDouble
                }

                val weightPrecision = if(weightTp + weightFp == 0d) {
                    1d 
                } else {
                    weightTp / (weightTp + weightFp).toDouble
                }

                val fpr = fp / (fp + tn).toDouble
                val weightFpr = weightFp / (weightFp + weightTn).toDouble

                currentRecord += 1
                currentTag += map.getOrElse("tag", 0d)
                currentWeight += map.getOrElse("weightColumn", 0d)
                currentWeightTag += map.getOrElse("weightTag", 0d)

                resultArray += Map(("actionRate", actionRate), ("weightActionRate", weightActionRate), ("recall", recall), ("weightRecall", weightRecall),
                    ("precision", precision), ("weightPrecision", weightPrecision), ("fpr", fpr), ("weightFpr", weightFpr), ("score", score), ("currentRecord", currentRecord),
                    ("tp", tp), ("tn", tn), ("fn", fn), ("fp", fp), ("weightTp", weightTp), ("weightTn", weightTn), ("weightFn", weightFn), ("weightFp", weightFp) 
                    )
            }
            val result = resultArray.reverse.slice(1, resultArray.length).foldLeft(ArrayBuffer(resultArray.last))((array : ArrayBuffer[Map[String, Double]], x : Map[String, Double]) => {
                var keep = false
                val lIterator = x.toIterator
                while(lIterator.hasNext && !keep) {
                    val (k, v) = lIterator.next
                    keep = k match {
                        case "score" => v.toInt - array.last.getOrElse(k, 0d).toInt != 0
                        case "currentRecord" => false
                        case _ => (v * numBucket).toInt - (array.last.getOrElse(k, 0d) * numBucket).toInt != 0
                    }
                }
                if(keep || !lIterator.hasNext) {
                    array += x
                }
                array
            })
            result.reverse.iterator
        })
        //TODO: change hard code path
        val perfResult = perfResultRDD.saveAsTextFile("hdfs:///user/website/wzhu1/" + key + "-perf.txt")
        sortedRDD.unpersist(true)
        perfResultRDD.collect
    }
    
    def sortByFieldName(name : String, scoreRDD : RDD[scala.collection.Map[String, Double]]) : RDD[Map[String, Double]] = {
        scoreRDD.map(map => map.filterKeys(key => Set(name, "weightTag", "tag", "weightColumn").contains(key))).sortBy(map => map.getOrElse(name, Double.NaN))
    }
}
