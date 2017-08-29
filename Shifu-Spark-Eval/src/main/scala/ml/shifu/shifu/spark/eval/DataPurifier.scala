package ml.shifu.shifu.spark.eval

import org.apache.spark.rdd.RDD
import org.apache.commons.jexl2.{Expression, JexlEngine, JexlException, MapContext}
import org.apache.commons.lang.StringUtils

trait DataPurifier {

        val inputRDD : RDD[String]

        val headers : Array[String]

        val delimiter : String

        val filterExp : String

        val context : SparkContext
        def init()

        def purify() : RDD[String] = {
            purify(intputRDD, headers, delimiter, filterExp)
        }

        

        private[this] def purify(inputRDD : RDD[String], headers : Array[String], delimiter : String, filterExp : Option[String]): RDD[String] = {
            filterExp match {
                case Some(exp) => inputRDD.mapPartitionsWithIndex { (index, iterator) => {
                        val jexl = new JexlEngine()
                        val jc = new MapContext()
                        val jexlExpression = try {
                            Some(jexl.createExpression(exp))
                        } catch {
                            case _ => None
                        }

                        if(index == 0) {
                            iterator.drop(1)
                        }
                        val filterFunc : (Array[String] => Boolean) = { inputData : Array[String] => {
                                jexlExpression match {
                                    case None => true
                                    case Some(exp) => {
                                        for(i <- 0 until headers.length) {
                                            jc.set(headers(i), inputData(i))
                                        }
                                        try {
                                            Boolean.unbox(exp.evaluate(jc))
                                        } catch {
                                            case _  => false
                                        }
                                    }
                                }
                            }
                        }
                        iterator.filter(line => filterFunc(StringUtils.split(line, delimiter)))
                    }
                }
                case None => inputRDD
            }
        }
}


