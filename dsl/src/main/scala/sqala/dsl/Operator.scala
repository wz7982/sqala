package sqala.dsl

import sqala.dsl.statement.query.*

import scala.NamedTuple.NamedTuple
import scala.annotation.targetName

extension [T](x: T)(using m: Merge[T])
    @targetName("eq")
    def ===[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) == mr.asExpr(that)

    @targetName("eq")
    def ===[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) == query

    @targetName("eq")
    def ===[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) == item

    @targetName("ne")
    def <>[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) != mr.asExpr(that)

    @targetName("ne")
    def <>[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) != query

    @targetName("ne")
    def <>[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) != item

    @targetName("gt")
    def >[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) > mr.asExpr(that)

    @targetName("gt")
    def >[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) > query

    @targetName("gt")
    def >[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) > item

    @targetName("ge")
    def >=[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) >= mr.asExpr(that)

    @targetName("ge")
    def >=[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) >= query

    @targetName("ge")
    def >=[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) >= item

    @targetName("lt")
    def <[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) < mr.asExpr(that)

    @targetName("lt")
    def <[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) < query

    @targetName("lt")
    def <[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) < item

    @targetName("le")
    def <=[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) <= mr.asExpr(that)

    @targetName("le")
    def <=[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) <= query

    @targetName("le")
    def <=[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) <= item

    def in[R, I <: Iterable[R]](list: I)(using
        a: ComparableValue[R],
        c: CompareOperation[m.R, R]
    ): Expr[Boolean] =
        m.asExpr(x).in(list)

    def in[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x).in(query)