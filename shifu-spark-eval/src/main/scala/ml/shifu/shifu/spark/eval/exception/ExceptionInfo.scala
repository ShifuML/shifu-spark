package ml.shifu.shifu.spark.eval.exception

object ExceptionInfo extends Enumeration {
    val InputError, NoScoreResult, IOException = Value
}

case class SparkEvalException(message : String, errorType : ExceptionInfo.Value, cause : Throwable = new Exception) extends java.lang.Exception(message, cause)

