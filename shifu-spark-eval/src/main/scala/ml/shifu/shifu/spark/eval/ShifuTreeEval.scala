package ml.shifu.shifu.spark.eval

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Accumulator, AccumulatorParam}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast

import ml.shifu.shifu.container.obj.{EvalConfig, ModelConfig}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.core.TreeModel
import ml.shifu.shifu.util.CommonUtils
import ml.shifu.shifu.exception.{ShifuException, ShifuErrorCode}
import ml.shifu.shifu.util.Constants

import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedList
import scala.collection.mutable.{Map, HashMap, HashSet}


class ShifuTreeEval(broadcastModelConfig : Broadcast[ModelConfig], broadcastEvalConfig : Broadcast[EvalConfig], context : SparkContext, 
    broadcastHeaders : Broadcast[Array[String]], accumMap : Map[String, Accumulator[Long]]) extends Eval with Serializable {

    def evalScore(filteredRdd : RDD[String]) : RDD[Map[String, Double]] = {
        val recordCounter = accumMap.getOrElseUpdate(Constants.COUNTER_RECORDS, new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_RECORDS)))
        val posCounter = accumMap.getOrElseUpdate(Constants.COUNTER_POSTAGS, new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_POSTAGS)))
        val weightPosCounter = accumMap.getOrElseUpdate(Constants.COUNTER_WPOSTAGS, new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_WPOSTAGS)))
        val negCounter = accumMap.getOrElseUpdate(Constants.COUNTER_NEGTAGS, new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_NEGTAGS)))
        val weightNegCounter = accumMap.getOrElseUpdate(Constants.COUNTER_WNEGTAGS, new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_WNEGTAGS)))
        filteredRdd.mapPartitions( iterator => {
            val modelConfig = broadcastModelConfig.value 
            val evalConfig = broadcastEvalConfig.value
            val headers = broadcastHeaders.value
            //val scale = evalConfig.getScoreScale
            val scale = 1000
            val models = CommonUtils.loadBasicModels(modelConfig, evalConfig, evalConfig.getDataSet.getSource, evalConfig.getGbtConvertToProb).asScala
            val resultsArray = new ArrayBuffer[Map[String, Double]]
            
            val posTag = new HashSet[String]
            posTag ++= modelConfig.getPosTags.asScala
            val negTag = new HashSet[String]
            negTag ++= modelConfig.getNegTags.asScala

            var recordNum = 0l
            var posNum = 0l
            var weightPos = 0l
            var negNum = 0l
            var weightNeg = 0l

            while(iterator.hasNext) {
               val line = iterator.next
               val inputMap = new java.util.HashMap[String, Object]
               val fields = CommonUtils.split(line, evalConfig.getDataSet.getDataDelimiter)
               for(i <- 0 until headers.length) {
                    Option(fields(i)) match {
                        case None => 
                            inputMap.put(headers(i), "")
                        case Some(field) => 
                            inputMap.put(headers(i), fields(i))
                    }
               }

               if(!inputMap.isEmpty()) {
                   val modelResults = new ArrayBuffer[Double] 
                   val outputResults = new HashMap[String, Double]
                   if(StringUtils.isNotBlank(evalConfig.getDataSet.getWeightColumnName) && 
                        inputMap.get(evalConfig.getDataSet.getWeightColumnName) != null) {
                        outputResults.put("weightColumn", inputMap.get(evalConfig.getDataSet.getWeightColumnName).toString.toDouble)
                        //outputResults += inputMap.get(evalConfig.getDataSet.getWeightColumnName).toString.toDouble
                   } else {
                        outputResults.put("weightColumn", 1.0d)
                        //outputResults += 1.0d
                   }
                   var modelIndex = 0
                   for(basicModel <- models) {
                        basicModel match {
                            case model : TreeModel => 
                                val independentTreeModel = model.getIndependentTreeModel
                                val score = independentTreeModel.compute(inputMap)
                                outputResults.put("model" + modelIndex, score(0) * scale)
                                modelIndex += 1
                                modelResults += score(0) * scale
                            case _ => 
                                modelResults
                        }
                   }
                   outputResults.put("max", modelResults.max)
                   outputResults.put("min", modelResults.min)
                   outputResults.put("avg", modelResults.sum / modelResults.size)
                   //outputResults += modelResults.min
                   //outputResults += modelResults.sum / modelResults.size
                   //outputResults ++= modelResults
                   for(metaColumn <- evalConfig.getAllMetaColumns(modelConfig).asScala) {
                        if(inputMap.get(metaColumn) != null) {
                            outputResults.put("meta:"+ metaColumn, inputMap.get(metaColumn).toString.toDouble)
                            //outputResults += inputMap.get(metaColumn).toString.toDouble
                        }
                   }
                   var tag = ""
                   try {
                        tag = CommonUtils.trimTag(inputMap.get(modelConfig.getTargetColumnName).toString)
                   } catch {
                        case _ => //throw new RuntimeException("null value is " + modelConfig.getTargetColumnName + " in "  + line)
                   }
                   recordNum += 1     
                   if(tag != null) {
                       if(posTag.contains(tag)) {
                            posNum += 1
                            weightPos += (outputResults.getOrElse("weightColumn", 1.0d) * Constants.EVAL_COUNTER_WEIGHT_SCALE).toLong
                            outputResults.put("tag", tag.toLong)
                            outputResults.put("weightTag", (outputResults.getOrElse("weightColumn", 1.0d) * Constants.EVAL_COUNTER_WEIGHT_SCALE).toLong)
                            resultsArray += outputResults
                       } else if(negTag.contains(tag)) {
                            negNum += 1
                            weightNeg += (outputResults.getOrElse("weightColumn", 1.0d) * Constants.EVAL_COUNTER_WEIGHT_SCALE).toLong
                            outputResults.put("tag", tag.toLong)
                            outputResults.put("weightTag", (0 * Constants.EVAL_COUNTER_WEIGHT_SCALE).toLong)
                            resultsArray += outputResults
                       }
                   }
               }
            }

        recordCounter.add(recordNum)
        posCounter.add(posNum)
        weightPosCounter.add(weightPos)
        negCounter.add(negNum)
        weightNegCounter.add(weightNeg)
        resultsArray.iterator
        })
    }
}

object ShifuTreeEval {

    def apply(modelConfig : Broadcast[ModelConfig], evalConfig : Broadcast[EvalConfig], context : SparkContext, headers : Broadcast[Array[String]], accumMap : Map[String,Accumulator[Long]]) = {
        new ShifuTreeEval(modelConfig, evalConfig, context, headers, accumMap) 
    }
}
