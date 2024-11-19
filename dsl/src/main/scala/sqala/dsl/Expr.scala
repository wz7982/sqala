package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.SqlOrderByNullsOption.{First, Last}
import sqala.ast.order.SqlOrderByOption.{Asc, Desc}
import sqala.ast.order.{SqlOrderBy, SqlOrderByNullsOption, SqlOrderByOption}
import sqala.ast.statement.SqlQuery
import sqala.dsl.statement.dml.UpdatePair
import sqala.dsl.statement.query.*

import java.util.Date
import scala.NamedTuple.NamedTuple
import scala.annotation.targetName

enum Expr[T] derives CanEqual:
    case Literal[T](value: T, a: AsSqlExpr[T]) extends Expr[T]

    case Column[T](tableName: String, columnName: String) extends Expr[T]

    case Null extends Expr[Nothing]

    case Binary[T](left: Expr[?], op: SqlBinaryOperator, right: Expr[?]) extends Expr[T]

    case Unary[T](expr: Expr[?], op: SqlUnaryOperator) extends Expr[T]

    case SubQuery[T](query: SqlQuery) extends Expr[T]

    case Func[T](
        name: String,
        args: List[Expr[?]],
        distinct: Boolean = false,
        orderBy: List[OrderBy[?]] = Nil,
        withinGroup: List[OrderBy[?]] = Nil,
        filter: Option[Expr[?]] = None
    ) extends Expr[T]

    case Case[T](
        branches: List[(Expr[?], Expr[?])],
        default: Expr[?]
    ) extends Expr[T]

    case Vector[T](items: List[Expr[?]]) extends Expr[T]

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
        orderBy: List[OrderBy[?]],
        frame: Option[SqlWindowFrame]
    ) extends Expr[T]

    case SubLink[T](query: SqlQuery, linkType: SqlSubLinkType) extends Expr[T]

    case Interval[T](value: Double, unit: SqlTimeUnit) extends Expr[T]

    case Cast[T](expr: Expr[?], castType: SqlCastType) extends Expr[T]

    case Extract[T](unit: SqlTimeUnit, expr: Expr[?]) extends Expr[T]

    case Ref[T](expr: Expr[?]) extends Expr[T]

    @targetName("eq")
    def ==[R](value: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, Equal, a.asExpr(value))

    @targetName("eq")
    def ==[R](that: Expr[R])(using
        CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, Equal, that)

    @targetName("eq")
    def ==[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        Binary(this, Equal, SubQuery(query.ast))

    @targetName("eq")
    def ==[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean] =
        Binary(this, Equal, SubLink(item.query, item.linkType))

    @targetName("ne")
    def !=[R](value: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, NotEqual, a.asExpr(value))

    @targetName("ne")
    def !=[R](that: Expr[R])(using
        CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, NotEqual, that)

    @targetName("ne")
    def !=[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        Binary(this, NotEqual, SubQuery(query.ast))

    @targetName("ne")
    def !=[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean] =
        Binary(this, NotEqual, SubLink(item.query, item.linkType))

    @targetName("gt")
    def >[R](value: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, GreaterThan, a.asExpr(value))

    @targetName("gt")
    def >[R](that: Expr[R])(using
        CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, GreaterThan, that)

    @targetName("gt")
    def >[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        Binary(this, GreaterThan, SubQuery(query.ast))

    @targetName("gt")
    def >[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean] =
        Binary(this, GreaterThan, SubLink(item.query, item.linkType))

    @targetName("ge")
    def >=[R](value: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, GreaterThanEqual, a.asExpr(value))

    @targetName("ge")
    def >=[R](that: Expr[R])(using
        CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, GreaterThanEqual, that)

    @targetName("ge")
    def >=[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        Binary(this, GreaterThanEqual, SubQuery(query.ast))

    @targetName("ge")
    def >=[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean] =
        Binary(this, GreaterThanEqual, SubLink(item.query, item.linkType))

    @targetName("lt")
    def <[R](value: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, LessThan, a.asExpr(value))

    @targetName("lt")
    def <[R](that: Expr[R])(using
        CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, LessThan, that)

    @targetName("lt")
    def <[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        Binary(this, LessThan, SubQuery(query.ast))

    @targetName("lt")
    def <[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean] =
        Binary(this, LessThan, SubLink(item.query, item.linkType))

    @targetName("le")
    def <=[R](value: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, LessThanEqual, a.asExpr(value))

    @targetName("le")
    def <=[R](that: Expr[R])(using
        CompareOperation[T, R]
    ): Expr[Boolean] =
        Binary(this, LessThanEqual, that)

    @targetName("le")
    def <=[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        Binary(this, LessThanEqual, SubQuery(query.ast))

    @targetName("le")
    def <=[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean] =
        Binary(this, LessThanEqual, SubLink(item.query, item.linkType))

    def in[R, I <: Iterable[R]](list: I)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        In(this, Vector(list.toList.map(a.asExpr(_))), false)

    def in[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        In(this, SubQuery(query.ast), false)

    def notIn[R, I <: Iterable[R]](list: I)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        In(this, Vector(list.toList.map(a.asExpr(_))), true)

    def notIn[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using
        m: Merge[V],
        a: AsExpr[V],
        c: CompareOperation[T, m.R]
    ): Expr[Boolean] =
        In(this, SubQuery(query.ast), true)

    def between[R](start: R, end: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Between(this, a.asExpr(start), a.asExpr(end), false)

    def between[S, E](start: Expr[S], end: Expr[E])(using
        CompareOperation[T, S],
        CompareOperation[T, E]
    ): Expr[Boolean] =
        Between(this, start, end, false)

    def notBetween[R](start: R, end: R)(using
        a: ComparableValue[R],
        o: CompareOperation[T, R]
    ): Expr[Boolean] =
        Between(this, a.asExpr(start), a.asExpr(end), true)

    def notBetween[S, E](start: Expr[S], end: Expr[E])(using
        CompareOperation[T, S],
        CompareOperation[T, E],
    ): Expr[Boolean] =
        Between(this, start, end, true)

    @targetName("plus")
    def +[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Binary(this, Plus, Literal(value, a))

    @targetName("plus")
    def +[R: Number](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Binary(this, Plus, that)

    @targetName("plus")
    def +(interval: TimeInterval)(using DateTime[T]): Expr[Option[Date]] =
        Binary(this, Plus, Interval(interval.value, interval.unit))

    @targetName("minus")
    def -[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Binary(this, Minus, Literal(value, a))

    @targetName("minus")
    def -[R](that: Expr[R])(using
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Binary(this, Minus, that)

    @targetName("minus")
    def -(interval: TimeInterval)(using DateTime[T]): Expr[Option[Date]] =
        Binary(this, Minus, Interval(interval.value, interval.unit))

    @targetName("times")
    def *[R: Number](value: R)(using
        n: Number[T],
        a: AsSqlExpr[R],
        r: ResultOperation[T, R]
    ): Expr[r.R] =
        Binary(this, Times, Literal(value, a))

    @targetName("times")
    def *[R: Number](that: Expr[R])(using
        n: Number[T],
        r: ResultOperation[T, R]
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

    def notLike(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        Binary(this, NotLike, Literal(value, summon[AsSqlExpr[String]]))

    def notLike[R <: String | Option[String]](that: Expr[R])(using T <:< (String | Option[String])): Expr[Boolean] =
        Binary(this, NotLike, that)

    def contains(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        like("%" + value + "%")

    def startsWith(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        like(value + "%")

    def endsWith(value: String)(using T <:< (String | Option[String])): Expr[Boolean] =
        like("%" + value)

    @targetName("json")
    def ->(n: Int)(using T <:< (Json | Option[Json])): Expr[Option[Json]] =
        Binary(this, Json, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("json")
    def ->(n: String)(using T <:< (Json | Option[Json])): Expr[Option[Json]] =
        Binary(this, Json, Literal(n, summon[AsSqlExpr[String]]))

    @targetName("jsonText")
    def ->>(n: Int)(using T <:< (Json | Option[Json])): Expr[Option[String]] =
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("jsonText")
    def ->>(n: String)(using T <:< (Json | Option[Json])): Expr[Option[String]] =
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[String]]))

object Expr:
    extension [T](expr: Expr[T])
        private[sqala] def asSqlExpr: SqlExpr = expr match
            case Literal(v, a) => a.asSqlExpr(v)
            case Column(tableName, columnName) =>
                SqlExpr.Column(Some(tableName), columnName)
            case Null => SqlExpr.Null
            case Binary(left, Equal, Literal(None, _)) =>
                SqlExpr.NullTest(left.asSqlExpr, false)
            case Binary(left, NotEqual, Literal(None, _)) =>
                SqlExpr.NullTest(left.asSqlExpr, true)
            case Binary(left, NotEqual, right@Literal(Some(_), _)) =>
                SqlExpr.Binary(
                    SqlExpr.Binary(left.asSqlExpr, NotEqual, right.asSqlExpr),
                    Or,
                    SqlExpr.NullTest(left.asSqlExpr, false)
                )
            case Binary(left, op, right) =>
                SqlExpr.Binary(left.asSqlExpr, op, right.asSqlExpr)
            case Unary(expr, op) =>
                SqlExpr.Unary(expr.asSqlExpr, op)
            case SubQuery(query) => SqlExpr.SubQuery(query)
            case Func(name, args, distinct, orderBy, withinGroup, filter) =>
                SqlExpr.Func(
                    name,
                    args.map(_.asSqlExpr),
                    distinct,
                    orderBy.map(_.asSqlOrderBy),
                    withinGroup.map(_.asSqlOrderBy),
                    filter.map(_.asSqlExpr)
                )
            case Case(branches, default) =>
                SqlExpr.Case(branches.map((x, y) => SqlCase(x.asSqlExpr, y.asSqlExpr)), default.asSqlExpr)
            case Vector(items) =>
                SqlExpr.Vector(items.map(_.asSqlExpr))
            case In(_, Vector(Nil), false) => SqlExpr.BooleanLiteral(false)
            case In(_, Vector(Nil), true) => SqlExpr.BooleanLiteral(true)
            case In(expr, inExpr, false) =>
                SqlExpr.Binary(expr.asSqlExpr, SqlBinaryOperator.In, inExpr.asSqlExpr)
            case In(expr, inExpr, true) =>
                SqlExpr.Binary(expr.asSqlExpr, SqlBinaryOperator.NotIn, inExpr.asSqlExpr)
            case Between(expr, start, end, not) =>
                SqlExpr.Between(expr.asSqlExpr, start.asSqlExpr, end.asSqlExpr, not)
            case Window(expr, partitionBy, orderBy, frame) =>
                SqlExpr.Window(expr.asSqlExpr, partitionBy.map(_.asSqlExpr), orderBy.map(_.asSqlOrderBy), frame)
            case SubLink(query, linkType) =>
                SqlExpr.SubLink(query, linkType)
            case Interval(value, unit) =>
                SqlExpr.Interval(value, unit)
            case Cast(expr, castType) =>
                SqlExpr.Cast(expr.asSqlExpr, castType)
            case Extract(unit, expr) =>
                SqlExpr.Extract(unit, expr.asSqlExpr)
            case Ref(expr) =>
                expr.asSqlExpr

    extension [T](expr: Expr[T])
        @targetName("to")
        def :=(value: T)(using a: AsSqlExpr[T]): UpdatePair = expr match
            case Column(_, columnName) =>
                UpdatePair(columnName, Literal(value, a))
            case _ =>
                UpdatePair("", Literal(value, a))

        @targetName("to")
        def :=[R](updateExpr: Expr[R])(using
            CompareOperation[T, R]
        ): UpdatePair = expr match
            case Column(_, columnName) =>
                UpdatePair(columnName, updateExpr)
            case _ =>
                UpdatePair("", updateExpr)

    extension [T](expr: Expr[T])
        infix def over(overValue: OverValue): Expr[T] =
            Expr.Window(expr, overValue.partitionBy, overValue.orderBy, overValue.frame)

        infix def over(overValue: Unit): Expr[T] =
            Expr.Window(expr, Nil, Nil, None)

    extension [T](expr: Expr[T])
        def asc: OrderBy[T] = OrderBy(expr, Asc, None)

        def desc: OrderBy[T] = OrderBy(expr, Desc, None)

        def ascNullsFirst: OrderBy[T] = OrderBy(expr, Asc, Some(First))

        def ascNullsLast: OrderBy[T] = OrderBy(expr, Asc, Some(Last))

        def descNullsFirst: OrderBy[T] = OrderBy(expr, Desc, Some(First))

        def descNullsLast: OrderBy[T] = OrderBy(expr, Desc, Some(Last))

    given exprToTuple1[T]: Conversion[Expr[T], Tuple1[Expr[T]]] = Tuple1(_)

class OrderBy[T](
    private[sqala] val expr: Expr[?],
    private[sqala] val order: SqlOrderByOption,
    private[sqala] val nullsOrder: Option[SqlOrderByNullsOption]
):
    private[sqala] def asSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.asSqlExpr, Some(order), nullsOrder)

class TimeInterval(private[sqala] val value: Double, private[sqala] val unit: SqlTimeUnit)

class SubLinkItem[T](private[sqala] val query: SqlQuery, private[sqala] val linkType: SqlSubLinkType)

case class OverValue(
    private[sqala] val partitionBy: List[Expr[?]] = Nil,
    private[sqala] val orderBy: List[OrderBy[?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    infix def orderBy(orderValue: OrderBy[?]*): OverValue =
        copy(orderBy = orderValue.toList)

    infix def rowsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): OverValue =
        copy(frame = Some(SqlWindowFrame.Rows(start, end)))

    infix def rangeBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): OverValue =
        copy(frame = Some(SqlWindowFrame.Range(start, end)))

    infix def groupsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): OverValue =
        copy(frame = Some(SqlWindowFrame.Groups(start, end)))

class WindowFunc[T](
   private[sqala] val name: String,
   private[sqala] val args: List[Expr[?]],
   private[sqala] val distinct: Boolean = false,
   private[sqala] val orderBy: List[OrderBy[?]] = Nil
)

object WindowFunc:
    extension [T](expr: WindowFunc[T])
        infix def over(overValue: OverValue): Expr[T] =
            val func = Expr.Func(expr.name, expr.args, expr.distinct, expr.orderBy)
            Expr.Window(func, overValue.partitionBy, overValue.orderBy, overValue.frame)

        infix def over(overValue: Unit): Expr[T] =
            val func = Expr.Func(expr.name, expr.args, expr.distinct, expr.orderBy)
            Expr.Window(func, Nil, Nil, None)
