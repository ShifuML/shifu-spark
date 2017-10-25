package ml.shifu.shifu.spark.eval.example

import ml.shifu.shifu.spark.eval.{Eval, ShifuEval}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.container.obj.ModelConfig

import org.apache.spark.{SparkContext, Accumulator}

import org.apache.hadoop.fs.Path

import scala.collection.mutable.HashMap

object TestShifuEval {

    def main(args : Array[String]) = {
        val context = new SparkContext
        val accumMap = new HashMap[String, Accumulator[Long]]
        val shifuEval = new ShifuEval(SourceType.LOCAL, "./ModelConfig.json", "./ColumnConfig.json", "Eval2", context, accumMap)
        val scoreRDD = shifuEval.eval
        scoreRDD.saveAsTextFile("hdfs://stampy/user/website/wzhu1-test-score")
        for(accumName <- accumMap.keySet) {
            Console.println("Accumulator : " + accumName + " is: " + accumMap.getOrElse(accumName, context.accumulator(0l)).value)
        }
        context.stop
    }
}
