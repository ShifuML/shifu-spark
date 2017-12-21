package ml.shifu.shifu.spark.eval.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import ml.shifu.shifu.util.CommonUtils
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.spark.eval.ShifuDataPurifier


object TestDataPurifier {

        def main(args : Array[String]) {
            val context = new SparkContext
            val purifier = ShifuDataPurifier("hdfs://stampy/user/website/wzhu1/ModelConfig.json", SourceType.HDFS, "hdfs://stampy/user/website/wzhu1-test-data", context)
            purifier.init
            val filteredRDD = purifier.purify()
            filteredRDD.saveAsTextFile("hdfs://stampy/user/website/wzhu1-filtered-data")
            context.stop
        }
}
