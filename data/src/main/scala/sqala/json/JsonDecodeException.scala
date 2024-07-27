package sqala.json

class JsonDecodeException(val msg: String = "The JSON does not match this data type.") extends Exception:
    override def toString: String =
        s"sqala.json.JsonDecodeException: \n$msg"