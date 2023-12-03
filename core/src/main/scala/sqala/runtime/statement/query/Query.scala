package sqala.runtime.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.{SqlQuery, SqlUnionType}
import sqala.runtime.*
import sqala.jdbc.Dialect

trait Query:
    def ast: SqlQuery

    infix def as(name: String): SubQueryTable = SubQueryTable(this, name, false)

    def toExpr: Expr = Expr(SqlExpr.SubQuery(ast))

    def sql(dialect: Dialect): (String, Array[Any]) =
        val printer = dialect.printer
        printer.printQuery(ast)
        printer.sql -> printer.args.toArray

    infix def union(query: Query): Union = Union(this, SqlUnionType.Union, query)

    infix def unionAll(query: Query): Union = Union(this, SqlUnionType.UnionAll, query)

    infix def except(query: Query): Union = Union(this, SqlUnionType.Except, query)

    infix def exceptAll(query: Query): Union = Union(this, SqlUnionType.ExceptAll, query)

    infix def intersect(query: Query): Union = Union(this, SqlUnionType.Intersect, query)

    infix def intersectAll(query: Query): Union = Union(this, SqlUnionType.IntersectAll, query)

case class LateralQuery(query: Query):
    infix def as(name: String): SubQueryTable = SubQueryTable(query, name, true)