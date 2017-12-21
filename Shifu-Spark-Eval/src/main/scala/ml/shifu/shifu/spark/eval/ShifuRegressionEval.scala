package ml.shifu.shifu.spark.eval

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Accumulator, AccumulatorParam}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast

import ml.shifu.shifu.container.obj.{EvalConfig, ModelConfig, ColumnConfig}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.column.NSColumn
import ml.shifu.shifu.core.ModelRunner
import ml.shifu.shifu.util.CommonUtils
import ml.shifu.shifu.exception.{ShifuException, ShifuErrorCode}
import ml.shifu.shifu.util.Constants

import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedList
import scala.collection.mutable.{Map, HashMap, HashSet}


class ShifuRegressionEval(broadcastModelConfig : Broadcast[ModelConfig], broadcastEvalConfig : Broadcast[EvalConfig], broadcastColumnConfigList : Broadcast[java.util.List[ColumnConfig]], context : SparkContext, 
    broadcastHeaders : Broadcast[Array[String]], accumMap : Map[String, Accumulator[Long]]) extends Eval with Serializable {

    def evalScore(filteredRdd : RDD[String]) : RDD[Map[String, Any]] = {
        Console.println("call regresion eval score")
        val recordCounter = accumMap.getOrElseUpdate(Constants.COUNTER_RECORDS, 
            new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_RECORDS)))

        val posCounter = accumMap.getOrElseUpdate(Constants.COUNTER_POSTAGS, 
            new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_POSTAGS)))

        val weightPosCounter = accumMap.getOrElseUpdate(Constants.COUNTER_WPOSTAGS, 
            new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_WPOSTAGS)))

        val negCounter = accumMap.getOrElseUpdate(Constants.COUNTER_NEGTAGS, 
            new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_NEGTAGS)))

        val weightNegCounter = accumMap.getOrElseUpdate(Constants.COUNTER_WNEGTAGS, 
            new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_WNEGTAGS)))

        filteredRdd.mapPartitions( iterator => {
            val modelConfig = broadcastModelConfig.value 
            val evalConfig = broadcastEvalConfig.value
            val headers = broadcastHeaders.value
            val columnConfigList =broadcastColumnConfigList.value
            val scale = evalConfig.getScoreScale
            val models = CommonUtils.loadBasicModels(modelConfig, evalConfig, evalConfig.getDataSet.getSource, evalConfig.getGbtConvertToProb)
            val resultsArray = new ArrayBuffer[Map[String, Any]]
            
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
               val inputMap = new java.util.HashMap[NSColumn, String]
               val fields = CommonUtils.split(line, evalConfig.getDataSet.getDataDelimiter)
               val modelRunner = new ModelRunner(modelConfig, columnConfigList, headers, evalConfig.getDataSet.getDataDelimiter, models)
               for(i <- 0 until headers.length) {
                    Option(fields(i)) match {
                        case None => 
                            val nsColumn = new NSColumn(headers(i))
                            inputMap.put(nsColumn, "")
                        case Some(field) => 
                            val nsColumn = new NSColumn(headers(i))
                            inputMap.put(nsColumn, fields(i))

                    }
               }
               if(!inputMap.isEmpty()) {
                   val modelResults = new ArrayBuffer[Double] 
                   val outputResults = new HashMap[String, Any]
                   if(StringUtils.isNotBlank(evalConfig.getDataSet.getWeightColumnName) && 
                        inputMap.get(new NSColumn(evalConfig.getDataSet.getWeightColumnName)) != null) {
                        outputResults.put("weightColumn", (inputMap.get(new NSColumn(evalConfig.getDataSet.getWeightColumnName)).toString.toDouble * Constants.EVAL_COUNTER_WEIGHT_SCALE).toLong)
                   } else {
                        outputResults.put("weightColumn", 1.0 * Constants.EVAL_COUNTER_WEIGHT_SCALE.toLong)
                   }
                   var modelIndex = 0
                   val cs = modelRunner.computeNsData(inputMap)
                   cs.getScores.asScala.foldLeft(0)((modelIndex : Int, score : java.lang.Double) => {
                        outputResults.put("model" + modelIndex, score) 
                        modelIndex + 1
                   })
                   outputResults.put("max", cs.getMaxScore)
                   outputResults.put("min", cs.getMinScore)
                   outputResults.put("avg", cs.getAvgScore)
                   outputResults.put("median", cs.getMedianScore)
                   for(metaColumn <- evalConfig.getAllMetaColumns(modelConfig).asScala) {
                        try {
                            if(inputMap.get(new NSColumn(metaColumn)) != null) {
                                outputResults.put("meta_"+ metaColumn, inputMap.get(new NSColumn(metaColumn)).toString.toDouble)
                            }
                        } catch {
                            case _ => Console.println(metaColumn + " no data")
                                None
                        }
                   }
                   var tag = ""
                   try {
                        tag = CommonUtils.trimTag(inputMap.get(new NSColumn(modelConfig.getTargetColumnName)).toString)
                   } catch {
                        case _ => //throw new RuntimeException("null value is " + modelConfig.getTargetColumnName + " in "  + line)
                            Unit
                   }
                   recordNum += 1     
                   if(tag != null) {
                       if(posTag.contains(tag)) {
                            posNum += 1
                            weightPos += outputResults.getOrElse("weightColumn", 1.0d * Constants.EVAL_COUNTER_WEIGHT_SCALE.toLong).toString.toDouble.toLong
                            outputResults.put("tag", tag)
                            outputResults.put("weightTag", (outputResults.getOrElse("weightColumn", 1.0d * Constants.EVAL_COUNTER_WEIGHT_SCALE.toLong)))
                            resultsArray += outputResults
                       } else if(negTag.contains(tag)) {
                            negNum += 1
                            weightNeg += (outputResults.getOrElse("weightColumn", 1.0d * Constants.EVAL_COUNTER_WEIGHT_SCALE.toLong)).toString.toDouble.toLong
                            outputResults.put("tag", tag)
                            outputResults.put("weightTag", 0d)
                            resultsArray += outputResults
                       } else {
                            outputResults.put("tag", tag)
                            outputResults.put("weightTag", 0d)
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

object ShifuRegressionEval {

    def apply(modelConfig : Broadcast[ModelConfig], evalConfig : Broadcast[EvalConfig], columnConfigList : Broadcast[java.util.List[ColumnConfig]], context : SparkContext, 
        headers : Broadcast[Array[String]], accumMap : Map[String,Accumulator[Long]]) = {

        new ShifuRegressionEval(modelConfig, evalConfig, columnConfigList, context, headers, accumMap) 
    }
}
