package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.SqlOrderByNullsOption.{First, Last}
import sqala.ast.order.SqlOrderByOption.{Asc, Desc}
import sqala.ast.order.{SqlOrderBy, SqlOrderByNullsOption, SqlOrderByOption}
import sqala.ast.statement.SqlQuery
import sqala.dsl.statement.dml.UpdatePair
import sqala.dsl.statement.query.Query

import scala.annotation.targetName

sealed trait ExprKind

case object ValueKind extends ExprKind
type ValueKind = ValueKind.type

case object CommonKind extends ExprKind
type CommonKind = CommonKind.type

case object ColumnKind extends ExprKind
type ColumnKind = ColumnKind.type

case object AggKind extends ExprKind
type AggKind = AggKind.type

case object WindowKind extends ExprKind
type WindowKind = WindowKind.type

enum Expr[T, K <: ExprKind] derives CanEqual:
    case Literal[T](value: T, a: AsSqlExpr[T]) extends Expr[T, ValueKind]
    case Column[T](tableName: String, columnName: String) extends Expr[T, ColumnKind]
    case Null extends Expr[Null, CommonKind]
    case Binary[T, K <: CompositeKind](left: Expr[?, ?], op: SqlBinaryOperator, right: Expr[?, ?]) extends Expr[T, K]
    case CustomBinary[T](left: Expr[?, ?], op: SqlBinaryOperator, right: Expr[?, ?]) extends Expr[T, CommonKind]
    case Unary[T, K <: CompositeKind](expr: Expr[?, ?], op: SqlUnaryOperator) extends Expr[T, K]
    case SubQuery[T](query: SqlQuery) extends Expr[T, CommonKind]
    case Func[T, K <: FuncKind](name: String, args: List[Expr[?, ?]], distinct: Boolean = false, orderBy: List[OrderBy[?]] = Nil) extends Expr[T, K]
    case Case[T, K <: CompositeKind](branches: List[(Expr[?, ?], Expr[?, ?])], default: Expr[?, ?]) extends Expr[T, K]
    case Vector[T](items: List[Expr[?, ?]]) extends Expr[T, CommonKind]
    case In[K <: CompositeKind](expr: Expr[?, ?], inExpr: Expr[?, ?], not: Boolean) extends Expr[Boolean, K]
    case Between[K <: CompositeKind](expr: Expr[?, ?], start: Expr[?, ?], end: Expr[?, ?], not: Boolean) extends Expr[Boolean, K]
    case Window[T](expr: Expr[?, ?], partitionBy: List[Expr[?, ?]], orderBy: List[OrderBy[?]], frame: Option[SqlWindowFrame]) extends Expr[T, WindowKind]
    case SubLink[T](query: SqlQuery, linkType: SqlSubLinkType) extends Expr[T, CommonKind]
    case Interval[T](value: Double, unit: SqlTimeUnit) extends Expr[T, ValueKind]
    case Cast[T, K <: CompositeKind](expr: Expr[?, ?], castType: String) extends Expr[T, K]
    case Extract[T, K <: CompositeKind](unit: SqlTimeUnit, expr: Expr[?, ?]) extends Expr[T, K]
    case Grouping(items: List[Expr[?, ?]]) extends Expr[Int, AggKind]

    private[sqala] def asSqlExpr: SqlExpr = this match
        case Literal(v, a) => a.asSqlExpr(v)
        case Column(tableName, columnName) =>
            SqlExpr.Column(Some(tableName), columnName)
        case Null => SqlExpr.Null
        case Binary(left, Equal, Literal(None, _)) =>
            SqlExpr.Binary(left.asSqlExpr, Is, SqlExpr.Null)
        case Binary(left, NotEqual, Literal(None, _)) =>
            SqlExpr.Binary(left.asSqlExpr, IsNot, SqlExpr.Null)
        case Binary(left, NotEqual, right@Literal(Some(_), _)) =>
            SqlExpr.Binary(SqlExpr.Binary(left.asSqlExpr, NotEqual, right.asSqlExpr), Or, SqlExpr.Binary(left.asSqlExpr, Is, SqlExpr.Null))
        case Binary(_, _, Literal(None, _)) =>
            SqlExpr.BooleanLiteral(false)
        case Binary(left, op, right) =>
            SqlExpr.Binary(left.asSqlExpr, op, right.asSqlExpr)
        case CustomBinary(left, op, right) =>
            SqlExpr.Binary(left.asSqlExpr, op, right.asSqlExpr)
        case Unary(expr, op) =>
            SqlExpr.Unary(expr.asSqlExpr, op)
        case SubQuery(query) => SqlExpr.SubQuery(query)
        case Func(name, args, distinct, orderBy) =>
            SqlExpr.Func(name, args.map(_.asSqlExpr), distinct, Map(), orderBy.map(_.asSqlOrderBy))
        case Case(branches, default) =>
            SqlExpr.Case(branches.map((x, y) => SqlCase(x.asSqlExpr, y.asSqlExpr)), default.asSqlExpr)
        case Vector(items) =>
            SqlExpr.Vector(items.map(_.asSqlExpr))
        case In(_, Vector(Nil), false) => SqlExpr.BooleanLiteral(false)
        case In(_, Vector(Nil), true) => SqlExpr.BooleanLiteral(true)
        case In(expr, inExpr, not) =>
            SqlExpr.In(expr.asSqlExpr, inExpr.asSqlExpr, not)
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
        case Grouping(items) =>
            SqlExpr.Grouping(items.map(_.asSqlExpr))

    @targetName("eq")
    def ==(value: T)(using a: AsSqlExpr[T]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, Equal, Literal(value, a))

    @targetName("eq")
    def ==[R <: Operation[T], RK <: OperationKind[K]](that: Expr[R, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, Equal, that)

    @targetName("eq")
    def ==[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, Equal, SubQuery(query.ast))

    @targetName("eq")
    def ==[R <: Operation[T]](item: SubLinkItem[R])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, Equal, SubLink(item.query, item.linkType))

    @targetName("ne")
    def !=(value: T)(using a: AsSqlExpr[T]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, NotEqual, Literal(value, a))

    @targetName("ne")
    def !=[R <: Operation[T], RK <: OperationKind[K]](that: Expr[R, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, NotEqual, that)

    @targetName("ne")
    def !=[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, NotEqual, SubQuery(query.ast))

    @targetName("ne")
    def !=[R <: Operation[T]](item: SubLinkItem[R])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, NotEqual, SubLink(item.query, item.linkType))

    @targetName("gt")
    def >(value: Unwrap[T, Option])(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThan, Literal(value, a))

    @targetName("gt")
    def >[R <: Operation[T], RK <: OperationKind[K]](that: Expr[R, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, GreaterThan, that)

    @targetName("gt")
    def >[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThan, SubQuery(query.ast))

    @targetName("gt")
    def >[R <: Operation[T]](item: SubLinkItem[R])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThan, SubLink(item.query, item.linkType))

    @targetName("ge")
    def >=(value: Unwrap[T, Option])(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThanEqual, Literal(value, a))

    @targetName("ge")
    def >=[R <: Operation[T], RK <: OperationKind[K]](that: Expr[R, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, GreaterThanEqual, that)

    @targetName("ge")
    def >=[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThanEqual, SubQuery(query.ast))

    @targetName("ge")
    def >=[R <: Operation[T]](item: SubLinkItem[R])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThanEqual, SubLink(item.query, item.linkType))

    @targetName("lt")
    def <(value: Unwrap[T, Option])(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThan, Literal(value, a))

    @targetName("lt")
    def <[R <: Operation[T], RK <: OperationKind[K]](that: Expr[R, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, LessThan, that)

    @targetName("lt")
    def <[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThan, SubQuery(query.ast))

    @targetName("lt")
    def <[R <: Operation[T]](item: SubLinkItem[R])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThan, SubLink(item.query, item.linkType))

    @targetName("le")
    def <=(value: Unwrap[T, Option])(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThanEqual, Literal(value, a))

    @targetName("le")
    def <=[R <: Operation[T], RK <: OperationKind[K]](that: Expr[R, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, LessThanEqual, that)

    @targetName("le")
    def <=[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThanEqual, SubQuery(query.ast))

    @targetName("le")
    def <=[R <: Operation[T]](item: SubLinkItem[R])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThanEqual, SubLink(item.query, item.linkType))

    def in(list: List[T])(using a: AsSqlExpr[T]): Expr[Boolean, ResultKind[K, ValueKind]] =
        In(this, Vector(list.map(Literal(_, a))), false)

    def in[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        In(this, SubQuery(query.ast), false)

    def in[I](exprs: I)(using CheckIn[I, T, K] =:= true): Expr[Boolean, ResultKind[K, ValueKind]] =
        val inExprs = exprs match
            case e: Expr[?, ?] => e :: Nil
            case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
        In(this, Vector(inExprs), false)

    def notIn(list: List[T])(using a: AsSqlExpr[T]): Expr[Boolean, ResultKind[K, ValueKind]] =
        In(this, Vector(list.map(Literal(_, a))), true)

    def notIn[R <: Operation[T], RK <: ExprKind](query: Query[Expr[R, RK]])(using (K == AggKind) =:= false): Expr[Boolean, ResultKind[K, ValueKind]] =
        In(this, SubQuery(query.ast), true)

    def notIn[I](exprs: I)(using CheckIn[I, T, K] =:= true): Expr[Boolean, ResultKind[K, ValueKind]] =
        val inExprs = exprs match
            case e: Expr[?, ?] => e :: Nil
            case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
        In(this, Vector(inExprs), true)

    def between(start: T, end: T)(using a: AsSqlExpr[T]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Between(this, Literal(start, a), Literal(end, a), false)

    def between[S <: Operation[T], SK <: OperationKind[K], E <: Operation[T], EK <: OperationKind[K]](start: Expr[S, SK], end: Expr[E, EK]): Expr[Boolean, ResultKind[K, SK]] =
        Between(this, start, end, false)

    def notBetween(start: T, end: T)(using a: AsSqlExpr[T]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Between(this, Literal(start, a), Literal(end, a), true)

    def notBetween[S <: Operation[T], SK <: OperationKind[K], E <: Operation[T], EK <: OperationKind[K]](start: Expr[S, SK], end: Expr[E, EK]): Expr[Boolean, ResultKind[K, SK]] =
        Between(this, start, end, true)

    @targetName("plus")
    def +(value: Unwrap[T, Option])(using n: Number[T], a: AsSqlExpr[Unwrap[T, Option]]): Expr[T, ResultKind[K, ValueKind]] =
        Binary(this, Plus, Literal(value, a))

    @targetName("plus")
    def +[R: Number, RK <: OperationKind[K]](that: Expr[R, RK])(using Number[T]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Plus, that)

    @targetName("minus")
    def -(value: Unwrap[T, Option])(using n: Number[T], a: AsSqlExpr[Unwrap[T, Option]]): Expr[T, ResultKind[K, ValueKind]] =
        Binary(this, Minus, Literal(value, a))

    @targetName("minus")
    def -[R: Number, RK <: OperationKind[K]](that: Expr[R, RK])(using Number[T]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Minus, that)

    @targetName("times")
    def *(value: Unwrap[T, Option])(using n: Number[T], a: AsSqlExpr[Unwrap[T, Option]]): Expr[T, ResultKind[K, ValueKind]] =
        Binary(this, Times, Literal(value, a))

    @targetName("times")
    def *[R: Number, RK <: OperationKind[K]](that: Expr[R, RK])(using Number[T]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Times, that)

    @targetName("div")
    def /(value: Unwrap[T, Option])(using n: Number[T], a: AsSqlExpr[Unwrap[T, Option]]): Expr[T, ResultKind[K, ValueKind]] =
        Binary(this, Div, Literal(value, a))

    @targetName("div")
    def /[R: Number, RK <: OperationKind[K]](that: Expr[R, RK])(using Number[T]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Div, that)

    @targetName("mod")
    def %(value: Unwrap[T, Option])(using n: Number[T], a: AsSqlExpr[Unwrap[T, Option]]): Expr[T, ResultKind[K, ValueKind]] =
        Binary(this, Mod, Literal(value, a))

    @targetName("mod")
    def %[R: Number, RK <: OperationKind[K]](that: Expr[R, RK])(using Number[T]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Mod, that)

    @targetName("positive")
    def unary_+(using Number[T]): Expr[T, ResultKind[K, ValueKind]] = Unary(this, Positive)

    @targetName("negative")
    def unary_-(using Number[T]): Expr[T, ResultKind[K, ValueKind]] = Unary(this, Negative)

    @targetName("and")
    def &&[RK <: OperationKind[K]](that: Expr[Boolean, RK])(using T =:= Boolean): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, And, that)

    @targetName("or")
    def ||[RK <: OperationKind[K]](that: Expr[Boolean, RK])(using T =:= Boolean): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, Or, that)

    @targetName("not")
    def unary_!(using T =:= Boolean): Expr[Boolean, ResultKind[K, ValueKind]] = Unary(this, Not)

    def like(value: String)(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, Like, Literal(value, summon[AsSqlExpr[String]]))

    def like[R <: String | Option[String], RK <: OperationKind[K]](that: Expr[R, RK])(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, Like, that)

    def notLike(value: String)(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, NotLike, Literal(value, summon[AsSqlExpr[String]]))

    def notLike[R <: String | Option[String], RK <: OperationKind[K]](that: Expr[R, RK])(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, NotLike, that)

    def contains(value: String)(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, ValueKind]] =
        like("%" + value + "%")

    def startsWith(value: String)(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, ValueKind]] =
        like(value + "%")

    def endsWith(value: String)(using T <:< (String | Option[String])): Expr[Boolean, ResultKind[K, ValueKind]] =
        like("%" + value)

    @targetName("json")
    def ->(n: Int)(using T <:< (String | Option[String])): Expr[Option[String], ResultKind[K, ValueKind]] =
        Binary(this, Json, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("json")
    def ->(n: String)(using T <:< (String | Option[String])): Expr[Option[String], ResultKind[K, ValueKind]] =
        Binary(this, Json, Literal(n, summon[AsSqlExpr[String]]))

    @targetName("jsonText")
    def ->>(n: Int)(using T <:< (String | Option[String])): Expr[Option[String], ResultKind[K, ValueKind]] =
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("jsonText")
    def ->>(n: String)(using T <:< (String | Option[String])): Expr[Option[String], ResultKind[K, ValueKind]] =
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[String]]))

    @targetName("plus")
    def +(interval: TimeInterval)(using DateTime[T]): Expr[Wrap[T, Option], ResultKind[K, ValueKind]] =
        Binary(this, Plus, Interval(interval.value, interval.unit))

    @targetName("minus")
    def -(interval: TimeInterval)(using DateTime[T]): Expr[Wrap[T, Option], ResultKind[K, ValueKind]] =
        Binary(this, Minus, Interval(interval.value, interval.unit))

    @targetName("timeDiff")
    def --[RT: DateTime, RK <: OperationKind[K]](that: Expr[RT, RK])(using DateTime[T]): Expr[Wrap[T, Option], ResultKind[K, RK]] =
        Binary(this, Minus, that)

object Expr:
    extension [T](expr: Expr[T, ColumnKind])
        @targetName("to")
        def :=(value: T)(using a: AsSqlExpr[T]): UpdatePair = expr match
            case Column(_, columnName) =>
                UpdatePair(columnName, Literal(value, a))

        @targetName("to")
        def :=[R <: Operation[T], K <: SimpleKind](updateExpr: Expr[R, K]): UpdatePair = expr match
            case Column(_, columnName) =>
                UpdatePair(columnName, updateExpr)

    extension [T](expr: Expr[T, AggKind])
        def over[P, O](partitionBy: P = EmptyTuple, orderBy: O = EmptyTuple, frame: Option[SqlWindowFrame] | SqlWindowFrame = None)(using CheckOverPartition[P] =:= true, CheckOverOrder[O] =:= true): Expr[T, WindowKind] =
            val partition = partitionBy match
                case e: Expr[?, ?] => e :: Nil
                case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
            val order = orderBy match
                case o: OrderBy[?] => o :: Nil
                case t: Tuple => t.toList.map(_.asInstanceOf[OrderBy[?]])
            val frameClause = frame match
                case o: Option[?] => o
                case o: SqlWindowFrame => Some(o)
            Window(expr, partition, order, frameClause)

    extension [T, K <: ExprKind](expr: Expr[T, K])
        def asc: OrderBy[K] = OrderBy(expr, Asc, None)

        def desc: OrderBy[K] = OrderBy(expr, Desc, None)

    extension [T, K <: ExprKind](expr: Expr[Option[T], K])
        def ascNullsFirst: OrderBy[K] = OrderBy(expr, Asc, Some(First))

        def ascNullsLast: OrderBy[K] = OrderBy(expr, Asc, Some(Last))

        def descNullsFirst: OrderBy[K] = OrderBy(expr, Desc, Some(First))

        def descNullsLast: OrderBy[K] = OrderBy(expr, Desc, Some(Last))

case class OrderBy[K <: ExprKind](expr: Expr[?, ?], order: SqlOrderByOption, nullsOrder: Option[SqlOrderByNullsOption]):
    private[sqala] def asSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.asSqlExpr, Some(order), nullsOrder)

case class TimeInterval(value: Double, unit: SqlTimeUnit)

case class SubLinkItem[T](query: SqlQuery, linkType: SqlSubLinkType)