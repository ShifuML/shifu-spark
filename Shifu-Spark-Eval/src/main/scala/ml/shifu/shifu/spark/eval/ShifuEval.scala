package ml.shifu.shifu.spark.eval

import ml.shifu.shifu.container.obj.{EvalConfig, ModelConfig, ColumnConfig}
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.util.CommonUtils
import ml.shifu.shifu.util.Constants
import ml.shifu.shifu.util.JSONUtils
import ml.shifu.shifu.exception.{ShifuException, ShifuErrorCode}
import ml.shifu.shifu.fs.PathFinder
import ml.shifu.shifu.container.meta.ValidateResult
import ml.shifu.shifu.core.validator.ModelInspector
import ml.shifu.shifu.core.validator.ModelInspector.ModelStep
import ml.shifu.shifu.column.NSColumn

import org.apache.spark.{SparkContext, SparkConf, Accumulator}
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.{ArrayBuffer, Set, HashSet, Map}

import java.io.File



class ShifuEval(sourceType : SourceType, modelConfigPath : String, columnConfigPath : String, 
    evalSetName : String, context : SparkContext, accumMap : Map[String, Accumulator[Long]]) {

    val modelConfig : ModelConfig = CommonUtils.loadModelConfig(modelConfigPath, sourceType)
    val evalConfig : EvalConfig = modelConfig.getEvalConfigByName(evalSetName)
    val columnConfigList : java.util.List[ColumnConfig] = CommonUtils.loadColumnConfigList(columnConfigPath, sourceType)
    val headers : Array[String] = CommonUtils.getFinalHeaders(evalConfig)
    var inputRDD : RDD[String] = context.textFile(evalConfig.getDataSet.getDataPath)
    val pathFinder : PathFinder = new PathFinder(modelConfig)

    def init() {
        validateModelConfig
        validateAlgorithmParam
        validateColumnConfig
        val conf : SparkConf = context.getConf
        //sync file from local to hdfs use the lib in shifu
        val evalSetPath = this.pathFinder.getEvalSetPath(this.evalConfig, SourceType.LOCAL)
        inputRDD = context.textFile(evalSetPath)
        FileUtils.forceMkdir(new File(evalSetPath))
        this.sourceType match {
            case SourceType.HDFS => CommonUtils.copyConfFromLocalToHDFS(this.modelConfig, this.pathFinder)
            case _ => false
        }
    }

    def validateModelConfig() {
        val result = Option(this.modelConfig) match {
            case Some(config) => ModelInspector.getInspector.probe(modelConfig, ModelStep.EVAL)
            case _ => 
                val validateResult = new ValidateResult
                    validateResult.getCauses.add("The model config is not loaded")
                    validateResult
        }
        if(!result.getStatus) {
            throw new ShifuException(ShifuErrorCode.ERROR_MODELCONFIG_NOT_VALIDATION)
        }
    }

    def validateColumnConfig() {
        val names = new HashSet[String] 
        for(header <- headers) {
            if(StringUtils.isEmpty(header)) {
                throw new IllegalArgumentException("Empty column name in csv, please check the header in your csv")
            }
        }
    }

    def validateAlgorithmParam() {
        val alg = this.modelConfig.getAlgorithm
        val checkAlgorith = () => {
            implicit class CaseInsensitiveRegex(sc: StringContext) {
              def ci = ( "(?i)" + sc.parts.mkString ).r
            }
            val paramMap = this.modelConfig.getParams
            alg match {
                //TODO : add check for other alg
                case ci"LR" =>
                    if(!paramMap.containsKey("LearningRate")) {
                        paramMap.clear
                        paramMap.put("LearningRate", new java.lang.Double(0.1))
                        saveModelConfig
                    }            
                case  ci"GBT" =>
                    if(!paramMap.containsKey("FeatureSubsetStrategy")) {
                        paramMap.clear
                        paramMap.put("FeatureSubsetStrategy", "all")
                        paramMap.put("MaxDepth", new java.lang.Integer(10))
                        paramMap.put("MaxStatsMemoryMB", new java.lang.Integer(256))
                        paramMap.put("Impurity", "entropy")
                        paramMap.put("Loss", "square")
                        saveModelConfig

                    }
                case _ => throw new ShifuException(ShifuErrorCode.ERROR_UNSUPPORT_ALG);
            }
        }
    }

    def saveModelConfig() {
        JSONUtils.writeValue(new File(this.pathFinder.getModelConfigPath(SourceType.LOCAL)), modelConfig)
    }
    
    def eval() : RDD[Map[String, Any]] = {
        val dataPurifier = ShifuDataPurifier(this.modelConfig, this.inputRDD, headers)
        val filteredRDD = dataPurifier.purify()
        val broadcastModelConfig = this.context.broadcast(this.modelConfig)
        val broadcastEvalConfig = this.context.broadcast(this.evalConfig)
        val broadcastHeaders = this.context.broadcast(this.headers)
        val broadcastColumnConfigList = this.context.broadcast(this.columnConfigList)
        val modelEval = modelConfig.isRegression match {
            case true => 
                Option(ShifuRegressionEval(broadcastModelConfig, broadcastEvalConfig, broadcastColumnConfigList, this.context, broadcastHeaders, this.accumMap))
            case _ => 
                Option(ShifuClassificationEval(broadcastModelConfig, broadcastEvalConfig, broadcastColumnConfigList, this.context, broadcastHeaders, this.accumMap))
        }
        modelEval match {
            case Some(shifuModelEval) => {
                shifuModelEval.evalScore(filteredRDD)
            }            
            case _ => throw new RuntimeException("ModelEval initialize failed.")
        }
    }
}
