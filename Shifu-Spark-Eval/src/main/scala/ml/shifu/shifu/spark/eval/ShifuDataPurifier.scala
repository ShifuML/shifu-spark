package ml.shifu.shifu.spark.eval

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.util.CommonUtils

class ShifuDataPurifier(modelConfig : ModelConfig, inputRDD : RDD[String], headers : Array[String])
    extends DataPurifier(inputRDD : RDD[String], headers : Array[String]) {

    override var delimiter : String = _

    override var filterExp : Option[String] = _

    override def init() {
        this.delimiter = modelConfig.getDataSetDelimiter
        this.filterExp = Option(modelConfig.getFilterExpressions)
    }
}

object ShifuDataPurifier {

    def apply(modelConfigPath : String, sourceType : SourceType, inputPath : String, context : SparkContext) : ShifuDataPurifier = {
        val modelConfig = CommonUtils.loadModelConfig(modelConfigPath, sourceType)
        val headers = CommonUtils.getFinalHeaders(modelConfig)
        val inputRDD = context.textFile(inputPath)
        new ShifuDataPurifier(modelConfig, inputRDD, headers)
    }

    def apply(modelConfig : ModelConfig, inputRDD : RDD[String], headers : Array[String]) = {
        new ShifuDataPurifier(modelConfig, inputRDD, headers)
    }
}
