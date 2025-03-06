package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.SqlOrderNullsOption.{First, Last}
import sqala.ast.order.SqlOrderOption.{Asc, Desc}
import sqala.ast.param.SqlParam
import sqala.ast.statement.SqlQuery
import sqala.common.*
import sqala.static.statement.dml.UpdatePair

import java.time.LocalDateTime
import scala.annotation.targetName
import scala.compiletime.ops.boolean.*

enum Expr[T]:
    case Literal[T](value: T, a: AsSqlExpr[T]) extends Expr[T]

    case Column[T](tableName: String, columnName: String) extends Expr[T]

    case Binary[T](left: Expr[?], op: SqlBinaryOperator, right: Expr[?]) extends Expr[T]

    case Unary[T](expr: Expr[?], op: SqlUnaryOperator) extends Expr[T]

    case SubQuery[T](query: SqlQuery) extends Expr[T]

    case Func[T](
        name: String,
        args: List[Expr[?]],
        distinct: Boolean = false,
        sortBy: List[Sort[?]] = Nil,
        withinGroup: List[Sort[?]] = Nil,
        filter: Option[Expr[?]] = None
    ) extends Expr[T]

    case Case[T](
        branches: List[(Expr[?], Expr[?])],
        default: Expr[?]
    ) extends Expr[T]

    case Tuple[T](items: List[Expr[?]]) extends Expr[T]

    case Array[T](items: List[Expr[?]]) extends Expr[T]

    case In(expr: Expr[?], inExpr: Expr[?], not: Boolean) extends Expr[Boolean]

    case Between(
        expr: Expr[?],
        start: Expr[?],
        end: Expr[?],
        not: Boolean
    ) extends Expr[Boolean]

    case Window[T](
        expr: Expr[?],
        partitionBy: List[Expr[?]],
        sortBy: List[Sort[?]],
        frame: Option[SqlWindowFrame]
    ) extends Expr[T]

    case SubLink[T](query: SqlQuery, linkType: SqlSubLinkType) extends Expr[T]

    case Interval[T](value: Double, unit: SqlTimeUnit) extends Expr[T]

    case Cast[T](expr: Expr[?], castType: SqlCastType) extends Expr[T]

    case Extract[T](unit: SqlTimeUnit, expr: Expr[?]) extends Expr[T]

    case Ref[T](expr: SqlExpr) extends Expr[T]

    @targetName("eq")
    def ==[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, Equal, a.asExpr(that))

    @targetName("eq")
    def ==[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, Equal, SubLink(item.query, item.linkType))

    @targetName("equal")
    def ===[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, Equal, a.asExpr(that))

    @targetName("equal")
    def ===[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, Equal, SubLink(item.query, item.linkType))

    @targetName("ne")
    def !=[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, NotEqual, a.asExpr(that))

    @targetName("ne")
    def !=[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, NotEqual, SubLink(item.query, item.linkType))

    @targetName("notEqual")
    def <>[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, NotEqual, a.asExpr(that))

    @targetName("notEqual")
    def <>[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, NotEqual, SubLink(item.query, item.linkType))

    @targetName("gt")
    def >[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, GreaterThan, a.asExpr(that))

    @targetName("gt")
    def >[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, GreaterThan, SubLink(item.query, item.linkType))

    @targetName("ge")
    def >=[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, GreaterThanEqual, a.asExpr(that))

    @targetName("ge")
    def >=[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, GreaterThanEqual, SubLink(item.query, item.linkType))

    @targetName("lt")
    def <[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, LessThan, a.asExpr(that))

    @targetName("lt")
    def <[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, LessThan, SubLink(item.query, item.linkType))

    @targetName("le")
    def <=[R](that: R)(using
        a: AsExpr[R],
        c: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, LessThanEqual, a.asExpr(that))

    @targetName("le")
    def <=[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, LessThanEqual, SubLink(item.query, item.linkType))

    def in[R](list: Seq[R])(using
        a: AsExpr[R],
        o: CompareOperation[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        In(this, Tuple(list.toList.map(a.asExpr(_))), false)

    def in[R](exprs: R)(using
        c: CanIn[T, R]
    ): Expr[Boolean] =
        In(this, c.asExpr(exprs), false)

    def between[S, E](start: S, end: E)(using
        as: AsExpr[S],
        cs: CompareOperation[Unwrap[T, Option], Unwrap[as.R, Option]],
        ae: AsExpr[E],
        ce: CompareOperation[Unwrap[T, Option], Unwrap[ae.R, Option]]
    ): Expr[Boolean] =
        Between(this, as.asExpr(start), ae.asExpr(end), false)

    @targetName("plus")
    def +[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R],
        r: ResultOperation[Unwrap[T, Option], Unwrap[R, Option], IsOption[T] || IsOption[R]]
    ): Expr[r.R] =
        Binary(this, Plus, Literal(value, a))

    @targetName("plus")
    def +[R: Number](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[Unwrap[T, Option], Unwrap[R, Option], IsOption[T] || IsOption[R]]
    ): Expr[r.R] =
        Binary(this, Plus, that)

    @targetName("plus")
    def +(interval: TimeInterval)(using
        d: DateTime[T],
        r: ResultOperation[Unwrap[T, Option], LocalDateTime, IsOption[T]]
    ): Expr[r.R] =
        Binary(this, Plus, Interval(interval.value, interval.unit))

    @targetName("minus")
    def -[R](value: R)(using
        a: AsSqlExpr[R],
        r: MinusOperation[Unwrap[T, Option], Unwrap[R, Option], IsOption[T] || IsOption[R]]
    ): Expr[r.R] =
        Binary(this, Minus, Literal(value, a))

    @targetName("minus")
    def -[R](that: Expr[R])(using
        r: MinusOperation[Unwrap[T, Option], Unwrap[R, Option], IsOption[T] || IsOption[R]]
    ): Expr[r.R] =
        Binary(this, Minus, that)

    @targetName("minus")
    def -(interval: TimeInterval)(using
        d: DateTime[T],
        r: ResultOperation[Unwrap[T, Option], LocalDateTime, IsOption[T]]
    ): Expr[r.R] =
        Binary(this, Minus, Interval(interval.value, interval.unit))

    @targetName("times")
    def *[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R],
        r: ResultOperation[Unwrap[T, Option], Unwrap[R, Option], IsOption[T] || IsOption[R]]
    ): Expr[r.R] =
        Binary(this, Times, Literal(value, a))

    @targetName("times")
    def *[R: Number](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[Unwrap[T, Option], Unwrap[R, Option], IsOption[T] || IsOption[R]]
    ): Expr[r.R] =
        Binary(this, Times, that)

    @targetName("div")
    def /[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R]
    ): Expr[Option[BigDecimal]] =
        Binary(this, Div, Literal(value, a))

    @targetName("div")
    def /[R: Number](that: Expr[R])(using
        Number[T]
    ): Expr[Option[BigDecimal]] =
        Binary(this, Div, that)

    @targetName("mod")
    def %[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R]
    ): Expr[Option[BigDecimal]] =
        Binary(this, Mod, Literal(value, a))

    @targetName("mod")
    def %[R: Number](that: Expr[R])(using
        Number[T]
    ): Expr[Option[BigDecimal]] =
        Binary(this, Mod, that)

    @targetName("positive")
    def unary_+(using Number[T]): Expr[T] = Unary(this, Positive)

    @targetName("negative")
    def unary_-(using Number[T]): Expr[T] = Unary(this, Negative)

    @targetName("and")
    def &&(that: Expr[Boolean])(using T =:= Boolean): Expr[Boolean] =
        Binary(this, And, that)

    @targetName("or")
    def ||(that: Expr[Boolean])(using T =:= Boolean): Expr[Boolean] =
        Binary(this, Or, that)

    @targetName("not")
    def unary_!(using T =:= Boolean): Expr[Boolean] =
        Unary(this, Not)

    def like(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        Binary(this, Like, Literal(value, summon[AsSqlExpr[String]]))

    def like[R <: String | Option[String]](that: Expr[R])(using T <:< (String | Option[String])): Expr[Boolean] =
        Binary(this, Like, that)

    def contains(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        like("%" + value + "%")

    def startWith(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        like(value + "%")

    def endWith(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        like("%" + value)

    @targetName("json")
    def ->(n: Int)(using T <:< (Json | Option[Json])): Expr[Option[Json]] =
        Binary(this, SqlBinaryOperator.Json, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("json")
    def ->(n: String)(using T <:< (Json | Option[Json])): Expr[Option[Json]] =
        Binary(this, SqlBinaryOperator.Json, Literal(n, summon[AsSqlExpr[String]]))

    @targetName("jsonText")
    def ->>(n: Int)(using T <:< (Json | Option[Json])): Expr[Option[String]] =
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("jsonText")
    def ->>(n: String)(using T <:< (Json | Option[Json])): Expr[Option[String]] =
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[String]]))

    def asc: Sort[T] = Sort(this, Asc, None)

    def desc: Sort[T] = Sort(this, Desc, None)

    def ascNullsFirst: Sort[T] = Sort(this, Asc, Some(First))

    def ascNullsLast: Sort[T] = Sort(this, Asc, Some(Last))

    def descNullsFirst: Sort[T] = Sort(this, Desc, Some(First))

    def descNullsLast: Sort[T] = Sort(this, Desc, Some(Last))

    infix def over(overValue: OverValue): Expr[T] =
        Expr.Window(this, overValue.partitionBy, overValue.sortBy, overValue.frame)

    infix def over(overValue: Unit): Expr[T] =
        Expr.Window(this, Nil, Nil, None)

    @targetName("to")
    def :=(value: T)(using a: AsSqlExpr[T]): UpdatePair = this match
        case Column(_, columnName) =>
            UpdatePair(columnName, Literal(value, a).asSqlExpr)
        case _ => throw MatchError(this)

    @targetName("to")
    def :=[R](updateExpr: Expr[R])(using
        CompareOperation[T, R]
    ): UpdatePair = this match
        case Column(_, columnName) =>
            UpdatePair(columnName, updateExpr.asSqlExpr)
        case _ => throw MatchError(this)

object Expr:
    extension [T](expr: Expr[T])
        private[sqala] def asSqlExpr: SqlExpr = expr match
            case Literal(v, a) => a.asSqlExpr(v)
            case Column(tableName, columnName) =>
                SqlExpr.Column(Some(tableName), columnName)
            case Binary(left, SqlBinaryOperator.Equal, Literal(None, _) | Ref(SqlExpr.Null)) =>
                SqlExpr.NullTest(left.asSqlExpr, false)
            case Binary(left, SqlBinaryOperator.NotEqual, Literal(None, _) | Ref(SqlExpr.Null)) =>
                SqlExpr.NullTest(left.asSqlExpr, true)
            case Binary(left, SqlBinaryOperator.NotEqual, right) =>
                SqlExpr.Binary(
                    SqlExpr.Binary(left.asSqlExpr, SqlBinaryOperator.NotEqual, right.asSqlExpr),
                    SqlBinaryOperator.Or,
                    SqlExpr.NullTest(left.asSqlExpr, false)
                )
            case Binary(left, op, right) =>
                SqlExpr.Binary(left.asSqlExpr, op, right.asSqlExpr)
            case Unary(expr, SqlUnaryOperator.Not) =>
                val sqlExpr = expr.asSqlExpr
                sqlExpr match
                    case SqlExpr.BooleanLiteral(boolean) =>
                        SqlExpr.BooleanLiteral(!boolean)
                    case SqlExpr.Binary(left, SqlBinaryOperator.Like, right) =>
                        SqlExpr.Binary(left, SqlBinaryOperator.NotLike, right)
                    case SqlExpr.Binary(left, SqlBinaryOperator.NotLike, right) =>
                        SqlExpr.Binary(left, SqlBinaryOperator.Like, right)
                    case SqlExpr.Binary(left, SqlBinaryOperator.In, right) =>
                        SqlExpr.Binary(left, SqlBinaryOperator.NotIn, right)
                    case SqlExpr.Binary(left, SqlBinaryOperator.NotIn, right) =>
                        SqlExpr.Binary(left, SqlBinaryOperator.In, right)
                    case SqlExpr.NullTest(query, n) =>
                        SqlExpr.NullTest(query, !n)
                    case SqlExpr.Between(expr, s, e, n) =>
                        SqlExpr.Between(expr, s, e, !n)
                    case _ => SqlExpr.Unary(sqlExpr, SqlUnaryOperator.Not)
            case Unary(expr, op) =>
                SqlExpr.Unary(expr.asSqlExpr, op)
            case SubQuery(query) => SqlExpr.SubQuery(query)
            case Func(name, args, distinct, sortBy, withinGroup, filter) =>
                SqlExpr.Func(
                    name,
                    args.map(_.asSqlExpr),
                    if distinct then Some(SqlParam.Distinct) else None,
                    sortBy.map(_.asSqlOrderBy),
                    withinGroup.map(_.asSqlOrderBy),
                    filter.map(_.asSqlExpr)
                )
            case Case(branches, default) =>
                SqlExpr.Case(branches.map((x, y) => SqlCase(x.asSqlExpr, y.asSqlExpr)), default.asSqlExpr)
            case Tuple(items) =>
                SqlExpr.Tuple(items.map(_.asSqlExpr))
            case Array(items) =>
                SqlExpr.Array(items.map(_.asSqlExpr))
            case In(_, Tuple(Nil), false) => SqlExpr.BooleanLiteral(false)
            case In(_, Tuple(Nil), true) => SqlExpr.BooleanLiteral(true)
            case In(expr, inExpr, false) =>
                SqlExpr.Binary(expr.asSqlExpr, SqlBinaryOperator.In, inExpr.asSqlExpr)
            case In(expr, inExpr, true) =>
                SqlExpr.Binary(expr.asSqlExpr, SqlBinaryOperator.NotIn, inExpr.asSqlExpr)
            case Between(expr, start, end, not) =>
                SqlExpr.Between(expr.asSqlExpr, start.asSqlExpr, end.asSqlExpr, not)
            case Window(expr, partitionBy, sortBy, frame) =>
                SqlExpr.Window(expr.asSqlExpr, partitionBy.map(_.asSqlExpr), sortBy.map(_.asSqlOrderBy), frame)
            case SubLink(query, linkType) =>
                SqlExpr.SubLink(query, linkType)
            case Interval(value, unit) =>
                SqlExpr.Interval(value, unit)
            case Cast(expr, castType) =>
                SqlExpr.Cast(expr.asSqlExpr, castType)
            case Extract(unit, expr) =>
                SqlExpr.Extract(unit, expr.asSqlExpr)
            case Ref(expr) => expr