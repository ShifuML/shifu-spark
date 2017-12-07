package ml.shifu.shifu.spark.eval

import ml.shifu.shifu.fs.ShifuFileUtils
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig}
import ml.shifu.shifu.core.eval.GainChartTemplate
import ml.shifu.shifu.spark.eval.exception._


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.Map

import java.text.DecimalFormat
import java.lang.{String => String}

object CSV {

    val DF = new DecimalFormat("#.########")
    
    def generateCSV(perfData : Array[Map[String, Double]], fileName : String) {
        val writer = ShifuFileUtils.getWriter(fileName, SourceType.LOCAL);
        try { 

            writer.write("ActionRate,WeightedActionRate,Recall,WeightedRecall,Precision,WeightedPrecision,FPR,WeightedFPR,BinLowestScore\n");
            val formatString = "%s,%s,%s,%s,%s,%s,%s,%s,%s\n";

            perfData.map(dataMap => {
                    writer.write(String.format(formatString, DF.format(dataMap.getOrElse("actionRate", 0d)), DF.format(dataMap.getOrElse("weightActionRate", 0d)),
                        DF.format(dataMap.getOrElse("recall", 0d)), DF.format(dataMap.getOrElse("weightRecall", 0d)), DF.format(dataMap.getOrElse("precision", 0d)), 
                        DF.format(dataMap.getOrElse("weightPrecision", 0d)), DF.format(dataMap.getOrElse("fpr", 0d)), DF.format(dataMap.getOrElse("weightFpr", 0d)),
                        dataMap.getOrElse("score", 0d).toString))
                })
        } catch {
            case e : Throwable => 
                e.printStackTrace
                writer.close
            case _ => 
                writer.close
        }
                writer.close
    }
}
