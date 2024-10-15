package sqala.optimizer.analysis

class AnalysisException(msg: String) extends Exception:
    override def toString: String =
        s"sqala.optimizer.analysis.AnalysisException: \n$msg"