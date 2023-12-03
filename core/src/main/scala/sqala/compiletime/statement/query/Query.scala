package sqala.compiletime.statement.query

import sqala.ast.statement.*
import sqala.compiletime.*
import sqala.compiletime.statement.ToSql
import sqala.jdbc.Dialect

trait Query[T <: Tuple, AliasNames <: Tuple] extends ToSql:
    def ast: SqlQuery

    private[sqala] def cols: List[Column[?, ?, ?]]

    infix def as(name: String)(using NonEmpty[name.type] =:= true, Repeat[AliasNames] =:= false): SubQueryTable[T, name.type, AliasNames] =
        SubQueryTable(this, name, false)

    private[sqala] def unsafeAs(name: String): SubQueryTable[T, name.type, AliasNames] = SubQueryTable(this, name, false)

    infix def union[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.Union, that)

    infix def unionAll[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.UnionAll, that)

    infix def ++[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.UnionAll, that)

    infix def except[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.Except, that)

    infix def exceptAll[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.ExceptAll, that)

    infix def intersect[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.Intersect, that)

    infix def intersectAll[UnionT <: Tuple](that: Query[UnionT, ?])(using CanUnion[T, UnionT] =:= true): Query[UnionTuple[T, UnionT], AliasNames] =
        new Union(this, SqlUnionType.IntersectAll, that)

    override def sql(dialect: Dialect): (String, Array[Any]) =
        val printer = dialect.printer
        printer.printQuery(ast)
        printer.sql -> printer.args.toArray