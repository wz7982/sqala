package sqala.static.dsl

final case class QueryContext[L <: Int](private[sqala] var tableIndex: Int):
    private[sqala] def fetchAlias: String =
        tableIndex += 1
        s"t$tableIndex"

final class OverContext

final class GroupingContext

final class ConnectByContext

final class GraphContext

final class PivotContext

final class MatchRecognizeContext

final class JsonContext