package sqala.static.dsl

case class QueryContext(var tableIndex: Int):
    private[sqala] def fetchAlias: String =
        tableIndex += 1
        s"t$tableIndex"