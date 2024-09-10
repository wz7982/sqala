package sqala.jdbc

class Record(record: Map[String, Any]) extends Selectable:
    def selectDynamic(name: String): Any = record(name)