package sqala.static.dsl

case class QueryContext(var tableIndex: Int):
    private[sqala] def fetchAlias: String =
        tableIndex += 1
        s"t$tableIndex"

class OverContext

class GroupingContext

class ConnectByContext

class GraphContext

class PivotContext

class MatchRecognizeContext

class JsonTableColumnContext