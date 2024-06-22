package sqala.dsl.statement.native

case class NativeSql(sql: String, args: Array[Any]):
    def +(that: NativeSql): NativeSql =
        NativeSql(sql + that.sql, args.appendedAll(that.args))

    def +(s: String): NativeSql =
        NativeSql(sql + s, args)