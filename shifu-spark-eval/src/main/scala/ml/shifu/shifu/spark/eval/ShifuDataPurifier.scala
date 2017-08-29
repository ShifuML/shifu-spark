package ml.shifu.shifu.spark.eval

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import ml.shifu.shifu.container.obj.EvalConfig
import ml.shifu.shifu.container.obj.ModelConfig
import ml.shifu.shifu.util.CommonUtils



public class ShifuDataPurifier(modelConfig : ModelConfig, inputPath : String) extends DataPurifier {

    override def init() {
        this.headers = CommonUtils.getFinalHeaders(modelConfig)
        this.delimieter = modelConfig.getDataSetDelimiter
        this.filterExp = modelConfig.getFilterExpression
        this.context = new SparkContext
        this.inputRDD = context.textFile(inputPath)
    }

}
