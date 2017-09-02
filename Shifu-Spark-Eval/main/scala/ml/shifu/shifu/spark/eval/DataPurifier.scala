package ml.shifu.shifu.spark.eval

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.commons.jexl2.{Expression, JexlEngine, JexlException, MapContext}
import com.google.common.base.Splitter
import scala.collection.JavaConverters._
import org.apache.commons.lang.StringUtils

trait DataPurifier {

    var inputRDD : RDD[String] = _

    var headers : Array[String] = _

    var delimiter : String = _

    var filterExp : Option[String] = _

    def init()

    def purify() : RDD[String] = {
        init
        purify(inputRDD, headers, delimiter, filterExp) 
    }

        

    private[this] def purify(inputRDD : RDD[String], headers : Array[String], delimiter : String, filterExp : Option[String]): RDD[String] = {
        filterExp match {
            case Some(exp) => inputRDD.mapPartitionsWithIndex { (index, iterator) => {
                val jexl = new JexlEngine()
                val jc = new MapContext()
                val jexlExpression = {
                    if(StringUtils.isBlank(exp)) {
                        None
                    } else {
                        try {
                        Option(jexl.createExpression(exp))
                        } catch {
                            case _ => None
                        }
                    }
                }

                    if(index == 0) {
                        iterator.drop(1)
                    }
                    val filterFunc : (Array[String] => Boolean) = { inputData : Array[String] => {
                        jexlExpression match {
                            case None => true
                            case Some(jexp) => {
                                if(headers.length != inputData.length) {
                                    false
                                } else {
                                    for(i <- 0 until headers.length) {
                                        jc.set(headers(i), inputData(i))
                                    }
                                    try {
                                        Boolean.unbox(jexp.evaluate(jc))
                                    } catch {
                                        case _  => false
                                    }
                                }
                            }
                        }
                    }}
                    iterator.filter(line => filterFunc(Splitter.on(delimiter).split(line).asScala.toArray))
            }}
            case _ => inputRDD
        }
    }
}


