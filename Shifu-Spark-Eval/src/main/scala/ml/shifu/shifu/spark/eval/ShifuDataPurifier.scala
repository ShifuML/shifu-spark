package ml.shifu.shifu.spark.eval

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.util.CommonUtils



class ShifuDataPurifier(modelConfig : ModelConfig, inputPath : String, context : SparkContext) extends DataPurifier {

    /*
    var inputRDD : RDD[String]

    var headers : Array[String]

    var delimiter : String

    var filterExp : Option[String]
    */
    override def init() {
        this.headers = CommonUtils.getFinalHeaders(modelConfig)
        this.delimiter = modelConfig.getDataSetDelimiter
        this.filterExp = Option(modelConfig.getFilterExpressions)
        this.inputRDD = context.textFile(inputPath)
    }

}


object ShifuDataPurifier {

    def apply(modelConfigPath : String, sourceType : SourceType, inputPath : String, context : SparkContext) : ShifuDataPurifier = {
        val modelConfig = CommonUtils.loadModelConfig(modelConfigPath, sourceType)
        new ShifuDataPurifier(modelConfig, inputPath, context)
    }
}
