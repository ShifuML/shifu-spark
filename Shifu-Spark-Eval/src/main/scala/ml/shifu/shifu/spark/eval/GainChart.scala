package ml.shifu.shifu.spark.eval

import ml.shifu.shifu.fs.ShifuFileUtils
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.container.obj.{ModelConfig, EvalConfig}
import ml.shifu.shifu.core.eval.GainChartTemplate
import ml.shifu.shifu.spark.eval.exception._
import ml.shifu.shifu.fs.PathFinder


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import java.io.BufferedWriter
import java.lang.{String => String}


object GainChart{

    def initChart(writer : BufferedWriter) {

        writer.write(GainChartTemplate.HIGHCHART_BASE_BEGIN)

        writer.write(String.format(GainChartTemplate.HIGHCHART_BUTTON_PANEL_TEMPLATE_1, "Weighted Operation Point",
                "lst0", "Weighted Recall", "lst1", "Unit-wise Recall"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_BUTTON_PANEL_TEMPLATE_2,
                "Unit-wise Operation Point", "lst2", "Weighted Recall", "lst3", "Unit-wise Recall"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_BUTTON_PANEL_TEMPLATE_3, "Model Score", "lst4",
                "Weighted Recall", "lst5", "Unit-wise Recall"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_BUTTON_PANEL_TEMPLATE_4, "Score Distibution",
                "lst6", "Score Count"))

        writer.write("      </div>\n")
        writer.write("      <div class=\"col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main\">\n")
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container0"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container1"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container2"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container3"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container4"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container5"))
        writer.write(String.format(GainChartTemplate.HIGHCHART_DIV, "container6"))

        writer.write("<script>\n")
        writer.write("\n")
        
    }

    private def end(writer : BufferedWriter, modelConfig : ModelConfig, evalConfig : EvalConfig, perfNum : Int, names : Array[String]) {
            writer.write("$(function () {\n")

            writer.write(String.format(GainChartTemplate.HIGHCHART_CHART_TEMPLATE_PREFIX, "container0",
                    "Weighted Recall", modelConfig.getBasic.getName, "Weighted  Operation Point", "%", "false"))
            var currIndex = 0
            writer.write("series: [")
            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")
            writer.write("\n")

            writer.write(String.format(GainChartTemplate.HIGHCHART_CHART_TEMPLATE_PREFIX, "container1",
                    "Unit-wise Recall", modelConfig.getBasic.getName, "Weighted  Operation Point", "%", "false"))
            writer.write("series: [")

            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")

            writer.write(String.format(GainChartTemplate.HIGHCHART_CHART_TEMPLATE_PREFIX, "container2",
                    "Weighted Recall", modelConfig.getBasic.getName, "Unit-wise  Operation Point", "%", "false"))
            writer.write("series: [")

            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")
            writer.write("\n")

            writer.write(String.format(GainChartTemplate.HIGHCHART_CHART_TEMPLATE_PREFIX, "container3",
                    "Unit-wise Recall", modelConfig.getBasic.getName, "Unit-wise  Operation Point", "%", "false"))
            writer.write("series: [")

            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")
            writer.write("\n")

            writer.write(String.format(GainChartTemplate.HIGHCHART_CHART_TEMPLATE_PREFIX, "container4",
                    "Weighted Recall", modelConfig.getBasic.getName, "Model Score", "", "true"))
            writer.write("series: [")

            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")
            writer.write("\n")

            writer.write(String.format(GainChartTemplate.HIGHCHART_CHART_TEMPLATE_PREFIX, "container5",
                    "Unit-wise Recall", modelConfig.getBasic.getName, "Model Score", "", "true"))
            writer.write("series: [")

            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")
            writer.write("\n")

            writer.write(String.format(GainChartTemplate.SCORE_HIGHCHART_CHART_PREFIX, "container6",
                    "Score Distribution", modelConfig.getBasic.getName, "Model Score", "", "false"))
            writer.write("series: [")

            for(i <- 0 to perfNum - 1) {
                writer.write("{")
                writer.write("  data: data_" + (currIndex) + ",")
                writer.write("  name: '" + names(i) + "',")
                writer.write("  turboThreshold:0")
                writer.write("}")
                if(i != perfNum - 1) {
                    writer.write(",")
                }
                currIndex += 1
            }
            writer.write("]")
            writer.write("});")
            writer.write("\n")

            writer.write("});\n")
            writer.write("\n")

            writer.write("$(document).ready(function () {\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst0", "container0", "lst0"))
            writer.write("\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst1", "container1", "lst1"))
            writer.write("\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst2", "container2", "lst2"))
            writer.write("\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst3", "container3", "lst3"))
            writer.write("\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst4", "container4", "lst4"))
            writer.write("\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst5", "container5", "lst5"))
            writer.write("\n")
            writer.write(String.format(GainChartTemplate.HIGHCHART_LIST_TOGGLE_TEMPLATE, "lst6", "container6", "lst6"))
            writer.write("\n")
            writer.write("  var ics = ['#container1', '#container2', '#container4', '#container5', '#container6'];\n")
            writer.write("  var icl = ics.length;\n")
            writer.write("  for (var i = 0; i < icl; i++) {\n")
            writer.write("      $(ics[i]).toggleClass('show');\n")
            writer.write("      $(ics[i]).toggleClass('hidden');\n")
            writer.write("      $(ics[i]).toggleClass('ls_chosen');\n")
            writer.write("  };\n")
            writer.write("\n")
            writer.write("});\n")
            writer.write("\n")
            writer.write("</script>\n")
            writer.write(GainChartTemplate.HIGHCHART_BASE_END)

    }

    private def drawWithData(perfData : Array[(String, Array[Map[String, Double]])], keys : Array[String], count : Int, writer : BufferedWriter,
        evalConfig : EvalConfig, modelConfig : ModelConfig) {

        perfData.foldLeft(count)((index , y) => { 
            writer.write("  var data_" + index + " = [\n")
            val sb = new StringBuilder("")
            y._2.map(x => {
                sb.append(String.format(GainChartTemplate.DATA_FORMAT,
                    GainChartTemplate.DF.format(x.getOrElse(keys(0), 0d) * 100),
                    GainChartTemplate.DF.format(x.getOrElse(keys(1), 0d) * 100),
                    GainChartTemplate.DF.format(x.getOrElse(keys(2), 0d) * 100),
                    GainChartTemplate.DF.format(x.getOrElse(keys(3), 0d) * 100),
                    GainChartTemplate.DF.format(x.getOrElse(keys(4), 0d) * 100),
                    GainChartTemplate.DF.format(x.getOrElse(keys(5), 0d))))
                sb.append(",")
            })
            writer.write(sb.dropRight(1).toString)
            writer.write("  ];\n")
            writer.write("\n")

            index + 1
        })
        
    }

    def genCSV(perfData : Array[(String, Array[Map[String, Double]])], modelConfig : ModelConfig, evalConfig : EvalConfig, key : String) {
        perfData.map(y => {
            val pathFinder = new PathFinder(modelConfig)
            val chartCSV = pathFinder.getEvalFilePath(evalConfig.getName, evalConfig.getName + "_" + y._1 + "_" + key + "_chart-spark.csv", SourceType.LOCAL)
            CSV.generateCSV(y._2, chartCSV) 
        })
    }

    def getPointsData(perfArrAsc : Array[Map[String, Double]], key : String, bucketNums : Int) : Array[Map[String, Double]] = {
        val perfArray = perfArrAsc.reverse 
        key match {
            case "score" => 
                perfArray.reverse.foldLeft(ArrayBuffer(perfArray.last))((arr, dataMap) => {
                    val score = dataMap.getOrElse("score", 0d)
                    if(score.toInt - arr.last.getOrElse("score", 0d).toInt != 0) {
                        arr += dataMap
                    }
                    arr
                }).toArray :+ perfArray.head
            case _ => 
                perfArray.foldLeft(ArrayBuffer(perfArray.head))((arr, dataMap) => {
                    val target = dataMap.getOrElse(key, 0d)
                    if((target * bucketNums).toInt - (arr.last.getOrElse(key, 0d) * bucketNums).toInt != 0) {
                        arr += dataMap
                    }
                    arr
                }).toArray
        }
    }

    def draw(perfArray : Array[(String, Array[Map[String, Double]])], fileName : String, modelConfig :ModelConfig, evalConfig : EvalConfig) {
         
        var writer : BufferedWriter = try {
            ShifuFileUtils.getWriter(fileName, SourceType.LOCAL)
        } catch {
            case ex => throw new SparkEvalException("Fail to get writer", ExceptionInfo.IOException, ex)
        }
        try {
            val count = perfArray.size
            val bucketNum = evalConfig.getPerformanceBucketNum
            initChart(writer)
            val chart0Data = perfArray.map(x => (x._1, getPointsData(x._2, "weightActionRate", bucketNum)))
            genCSV(chart0Data, modelConfig, evalConfig, "weight_gain")

            val chart0Keys = Array("weightRecall", "weightActionRate", "weightActionRate", "weightPrecision", "actionRate", "score")
            drawWithData(chart0Data, chart0Keys, 0 * count, writer, evalConfig, modelConfig)
            
            val chart1Keys = Array("recall", "weightActionRate", "weightActionRate", "precision", "actionRate", "score")
            drawWithData(chart0Data, chart1Keys, 1 * count, writer, evalConfig, modelConfig) 

            val chart2Data = perfArray.map(x => (x._1 ,getPointsData(x._2, "actionRate", bucketNum)))
            genCSV(chart2Data, modelConfig, evalConfig, "gain")

            val chart2Keys = Array("weightRecall", "actionRate", "weightActionRate", "weightPrecision", "actionRate", "score")
            drawWithData(chart2Data, chart2Keys, 2 * count, writer, evalConfig, modelConfig) 

            val chart3Keys = Array("recall", "actionRate", "weightActionRate", "precision", "actionRate", "score")
            drawWithData(chart2Data, chart3Keys, 3 * count, writer, evalConfig, modelConfig) 

            val chart4Data = perfArray.map(x => (x._1, getPointsData(x._2, "score", 1)))
            val chart4Keys = Array("weightRecall", "score", "weightActionRate", "weightPrecision", "actionRate", "score")
            drawWithData(chart4Data, chart4Keys, 4 * count, writer, evalConfig, modelConfig) 
            val chart5Keys = Array("weightRecall", "score", "weightActionRate", "weightPrecision", "actionRate", "score")
            drawWithData(chart4Data, chart5Keys, 5 * count, writer, evalConfig, modelConfig) 
            val chart6Keys = Array("weightRecall", "score", "weightActionRate", "weightPrecision", "actionRate", "score")
            drawWithData(chart4Data, chart6Keys, 6 * count, writer, evalConfig, modelConfig) 
            end(writer, modelConfig, evalConfig, count, perfArray.map(x => x._1))
            writer.close
        } catch {
            case ex => writer.close
                throw new SparkEvalException(ex.getMessage, ExceptionInfo.IOException, ex)
        }
    }

}
