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

class ShifuClassificationEval(broadcastModelConfig : Broadcast[ModelConfig], broadcastEvalConfig : Broadcast[EvalConfig], broadcastColumnConfigList : Broadcast[java.util.List[ColumnConfig]], context : SparkContext, 
    broadcastHeaders : Broadcast[Array[String]], accumMap : Map[String, Accumulator[Long]]) extends Eval with Serializable {

    def evalScore(filteredRdd : RDD[String]) : RDD[Map[String, Any]] = {
        val recordCounter = accumMap.getOrElseUpdate(Constants.COUNTER_RECORDS, 
            new Accumulator(0l, AccumulatorParam.LongAccumulatorParam, Option(Constants.COUNTER_RECORDS)))

        filteredRdd.mapPartitions( iterator => {
            val modelConfig = broadcastModelConfig.value 
            val evalConfig = broadcastEvalConfig.value
            val headers = broadcastHeaders.value
            val columnConfigList =broadcastColumnConfigList.value
            val scale = evalConfig.getScoreScale
            val models = CommonUtils.loadBasicModels(modelConfig, evalConfig, evalConfig.getDataSet.getSource, evalConfig.getGbtConvertToProb)
            Console.println("models count is: " + models.size)
            val resultsArray = new ArrayBuffer[Map[String, Any]]

            var recordNum = 0l

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
                   val cs = modelRunner.computeNsData(inputMap)
                   outputResults.put("scores", cs.getScores)
                   for(metaColumn <- evalConfig.getAllMetaColumns(modelConfig).asScala) {
                        if(inputMap.get(metaColumn) != null) {
                            outputResults.put("meta:"+ metaColumn, inputMap.get(new NSColumn(metaColumn)).toString)
                        }
                   }
                   outputResults.put("tag", CommonUtils.trimTag(inputMap.get(new NSColumn(modelConfig.getTargetColumnName))).toString)
                   recordNum += 1
                   Console.println("ourputResults:")
                   Console.println(outputResults)
                   resultsArray += outputResults
               }
            }

            recordCounter.add(recordNum)
            resultsArray.iterator
        })
    }
}

object ShifuClassificationEval {

    def apply(modelConfig : Broadcast[ModelConfig], evalConfig : Broadcast[EvalConfig], columnConfigList : Broadcast[java.util.List[ColumnConfig]], context : SparkContext, headers : Broadcast[Array[String]], accumMap : Map[String,Accumulator[Long]]) = {
        new ShifuClassificationEval(modelConfig, evalConfig, columnConfigList, context, headers, accumMap) 
    }
}
