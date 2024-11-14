package sqala.dsl

import sqala.ast.expr.SqlBinaryOperator.*
import sqala.dsl.statement.query.*

import scala.NamedTuple.NamedTuple
import scala.annotation.targetName

extension [T](value: T)(using as: AsSqlExpr[T])
    @targetName("eq")
    def ===[R](that: Expr[R])(using
        c: CompareOperation[T, R]
    ): Expr[Boolean] =
        Expr.Binary(Expr.Literal(value, as), Equal, that)

    @targetName("ne")
    def <>[R](that: Expr[R])(using
        c: CompareOperation[T, R]
    ): Expr[Boolean] =
        Expr.Binary(Expr.Literal(value, as), NotEqual, that)

    @targetName("gt")
    def >[R](that: Expr[R])(using
        c: CompareOperation[T, R]
    ): Expr[Boolean] =
        Expr.Binary(Expr.Literal(value, as), GreaterThan, that)

    @targetName("ge")
    def >=[R](that: Expr[R])(using
        c: CompareOperation[T, R]
    ): Expr[Boolean] =
        Expr.Binary(Expr.Literal(value, as), GreaterThanEqual, that)

    @targetName("lt")
    def <[R](that: Expr[R])(using
        c: CompareOperation[T, R]
    ): Expr[Boolean] =
        Expr.Binary(Expr.Literal(value, as), LessThan, that)

    @targetName("le")
    def <=[R](that: Expr[R])(using
        c: CompareOperation[T, R]
    ): Expr[Boolean] =
        Expr.Binary(Expr.Literal(value, as), LessThanEqual, that)

    @targetName("plus")
    def +[R: Number](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Expr.Binary(Expr.Literal(value, as), Plus, that)

    @targetName("minus")
    def -[R](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Expr.Binary(Expr.Literal(value, as), Minus, that)

    @targetName("times")
    def *[R: Number](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Expr.Binary(Expr.Literal(value, as), Times, that)

    @targetName("div")
    def /[R: Number](that: Expr[R])(using
        Number[T]
    ): Expr[Option[BigDecimal]] =
        Expr.Binary(Expr.Literal(value, as), Div, that)

    @targetName("mod")
    def %[R: Number](that: Expr[R])(using
        Number[T]
    ): Expr[Option[BigDecimal]] =
        Expr.Binary(Expr.Literal(value, as), Mod, that)

extension [T](x: T)(using m: Merge[T])
    @targetName("eq")
    def ===[R](that: R)(using
        mr: Merge[R],
        c: CompareOperation[m.R, mr.R]
    ): Expr[Boolean] =
        m.asExpr(x) == mr.asExpr(that)

    @targetName("eq")
    inline def ===[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
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
    inline def <>[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
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
    inline def >[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
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
    inline def >=[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
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
    inline def <[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x) < query

    @targetName("lt")
    def <[R](item: SubLinkItem[R])(using CompareOperation[m.R, R]): Expr[Boolean] =
        m.asExpr(x) < item

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

    def notIn[R, I <: Iterable[R]](list: I)(using
        a: ComparableValue[R],
        c: CompareOperation[m.R, R]
    ): Expr[Boolean] =
        m.asExpr(x).notIn(list)

    def notIn[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        mv: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[m.R, mv.R]
    ): Expr[Boolean] =
        m.asExpr(x).notIn(query)