package ml.shifu.shifu.spark.eval.example

import ml.shifu.shifu.spark.eval.{Eval, ShifuEval, RegressionPerformanceGenerator}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.util.{CommonUtils, Constants}

import org.apache.spark.{SparkContext, Accumulator}

import org.apache.hadoop.fs.Path

import scala.collection.mutable.HashMap

object TestPerformanceGenerator {

    def main(arg : Array[String]) {

        val context = new SparkContext
        val accumMap = new HashMap[String, Accumulator[Long]]
        val shifuEval = new ShifuEval(SourceType.LOCAL, "./ModelConfig.json", "./ColumnConfig.json", "Eval2", context, accumMap)
        val broadcastModelConfig = context.broadcast(shifuEval.modelConfig)
        val broadcastEvalConfig = context.broadcast(shifuEval.evalConfig)
        val modelNum = CommonUtils.getBasicModelsCnt(shifuEval.modelConfig, shifuEval.evalConfig, shifuEval.evalConfig.getDataSet.getSource)
        val scoreRDD = shifuEval.eval
        val perfGen = new RegressionPerformanceGenerator(broadcastModelConfig, broadcastEvalConfig, context, accumMap, modelNum)
        perfGen.genPerfByModels(scoreRDD)
        context.stop
    }
}
