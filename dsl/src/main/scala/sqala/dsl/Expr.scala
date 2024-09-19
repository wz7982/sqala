package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.SqlOrderByNullsOption.{First, Last}
import sqala.ast.order.SqlOrderByOption.{Asc, Desc}
import sqala.ast.order.{SqlOrderBy, SqlOrderByNullsOption, SqlOrderByOption}
import sqala.ast.statement.SqlQuery
import sqala.dsl.statement.dml.UpdatePair
import sqala.dsl.statement.query.{Query, ResultSize}

import scala.annotation.targetName
import scala.compiletime.{erasedValue, error}

enum Expr[T, K <: ExprKind] derives CanEqual:
    case Literal[T](value: T, a: AsSqlExpr[T]) extends Expr[T, ValueKind]
    case Column[T](tableName: String, columnName: String) extends Expr[T, ColumnKind]
    case Null extends Expr[Nothing, ValueKind]
    case Binary[T, K <: CompositeKind](left: Expr[?, ?], op: SqlBinaryOperator, right: Expr[?, ?]) extends Expr[T, K]
    case Unary[T, K <: CompositeKind](expr: Expr[?, ?], op: SqlUnaryOperator) extends Expr[T, K]
    case SubQuery[T](query: SqlQuery) extends Expr[T, CommonKind]
    case Func[T, K <: FuncKind](
        name: String,
        args: List[Expr[?, ?]],
        distinct: Boolean = false,
        orderBy: List[OrderBy[?, ?]] = Nil,
        withinGroupOrderBy: List[OrderBy[?, ?]] = Nil,
        filter: Option[Expr[?, ?]] = None
    ) extends Expr[T, K]
    case Case[T, K <: CompositeKind](branches: List[(Expr[?, ?], Expr[?, ?])], default: Expr[?, ?]) extends Expr[T, K]
    case Vector[T, K <: CompositeKind | ValueKind](items: List[Expr[?, ?]]) extends Expr[T, K]
    case In[K <: CompositeKind](expr: Expr[?, ?], inExpr: Expr[?, ?], not: Boolean) extends Expr[Boolean, K]
    case Between[K <: CompositeKind](expr: Expr[?, ?], start: Expr[?, ?], end: Expr[?, ?], not: Boolean) extends Expr[Boolean, K]
    case Window[T](expr: Expr[?, ?], partitionBy: List[Expr[?, ?]], orderBy: List[OrderBy[?, ?]], frame: Option[SqlWindowFrame]) extends Expr[T, WindowKind]
    case SubLink[T](query: SqlQuery, linkType: SqlSubLinkType) extends Expr[T, CommonKind]
    case Interval[T](value: Double, unit: SqlTimeUnit) extends Expr[T, ValueKind]
    case Cast[T, K <: CompositeKind](expr: Expr[?, ?], castType: String) extends Expr[T, K]
    case Extract[T, K <: CompositeKind](unit: SqlTimeUnit, expr: Expr[?, ?]) extends Expr[T, K]
    case Grouping(items: List[Expr[?, ?]]) extends Expr[Int, AggOperationKind]

    @targetName("eq")
    def ==[R](value: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, Equal, a.asExpr(value))

    @targetName("eq")
    def ==[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[T, R], KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, Equal, that)

    @targetName("eq")
    inline def ==[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using AsExpr[Q], CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        inline erasedValue[S] match
            case _: ResultSize.ManyRows =>
                error("Subquery must return only one row.")
            case _ =>
        Binary(this, Equal, SubQuery(query.ast))

    @targetName("eq")
    inline def ==[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        Binary(this, Equal, SubLink(item.query, item.linkType))

    @targetName("ne")
    def !=[R](value: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, NotEqual, a.asExpr(value))

    @targetName("ne")
    def !=[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[T, R], KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, NotEqual, that)

    @targetName("ne")
    inline def !=[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using AsExpr[Q], CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        inline erasedValue[S] match
            case _: ResultSize.ManyRows =>
                error("Subquery must return only one row.")
            case _ =>
        Binary(this, NotEqual, SubQuery(query.ast))

    @targetName("ne")
    inline def !=[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        Binary(this, NotEqual, SubLink(item.query, item.linkType))

    @targetName("gt")
    def >[R](value: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThan, a.asExpr(value))

    @targetName("gt")
    def >[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[T, R], KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, GreaterThan, that)

    @targetName("gt")
    inline def >[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using AsExpr[Q], CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        inline erasedValue[S] match
            case _: ResultSize.ManyRows =>
                error("Subquery must return only one row.")
            case _ =>
        Binary(this, GreaterThan, SubQuery(query.ast))

    @targetName("gt")
    inline def >[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        Binary(this, GreaterThan, SubLink(item.query, item.linkType))

    @targetName("ge")
    def >=[R](value: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, GreaterThanEqual, a.asExpr(value))

    @targetName("ge")
    def >=[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[T, R], KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, GreaterThanEqual, that)

    @targetName("ge")
    inline def >=[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using AsExpr[Q], CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        inline erasedValue[S] match
            case _: ResultSize.ManyRows =>
                error("Subquery must return only one row.")
            case _ =>
        Binary(this, GreaterThanEqual, SubQuery(query.ast))

    @targetName("ge")
    inline def >=[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        Binary(this, GreaterThanEqual, SubLink(item.query, item.linkType))

    @targetName("lt")
    def <[R](value: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThan, a.asExpr(value))

    @targetName("lt")
    def <[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[T, R], KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, LessThan, that)

    @targetName("lt")
    inline def <[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using AsExpr[Q], CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        inline erasedValue[S] match
            case _: ResultSize.ManyRows =>
                error("Subquery must return only one row.")
            case _ =>
        Binary(this, LessThan, SubQuery(query.ast))

    @targetName("lt")
    inline def <[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        Binary(this, LessThan, SubLink(item.query, item.linkType))

    @targetName("le")
    def <=[R](value: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Binary(this, LessThanEqual, a.asExpr(value))

    @targetName("le")
    def <=[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[T, R], KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        Binary(this, LessThanEqual, that)

    @targetName("le")
    inline def <=[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using AsExpr[Q], CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        inline erasedValue[S] match
            case _: ResultSize.ManyRows =>
                error("Subquery must return only one row.")
            case _ =>
        Binary(this, LessThanEqual, SubQuery(query.ast))

    @targetName("le")
    inline def <=[R](item: SubLinkItem[R])(using CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        Binary(this, LessThanEqual, SubLink(item.query, item.linkType))

    def in[R](list: List[R])(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        In(this, Vector(list.map(a.asExpr(_))), false)

    inline def in[R <: Tuple](expr: R)(using a: AsExpr[R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        CompareOperation.summonInstances[T, R]
        KindOperation.summonInstances[K, R]
        In(this, Vector(a.asExprs(expr)), false)

    inline def in[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        In(this, SubQuery(query.ast), false)

    def notIn[R](list: List[R])(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        In(this, Vector(list.map(a.asExpr(_))), true)

    inline def notIn[R <: Tuple](expr: R)(using a: AsExpr[R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        CompareOperation.summonInstances[T, R]
        KindOperation.summonInstances[K, R]
        In(this, Vector(a.asExprs(expr)), true)

    inline def notIn[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q])(using CompareOperation[T, m.R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[K] match
            case _: AggKind => 
                error("Aggregate function cannot be compared with subquery.")
            case _: AggOperationKind =>
                error("Aggregate function cannot be compared with subquery.")
            case _ =>
        In(this, SubQuery(query.ast), true)

    def between[R](start: R, end: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Between(this, a.asExpr(start), a.asExpr(end), false)

    def between[S, SK <: ExprKind, E, EK <: ExprKind](start: Expr[S, SK], end: Expr[E, EK])(using CompareOperation[T, S], KindOperation[K, SK], CompareOperation[T, E], KindOperation[K, EK]): Expr[Boolean, ResultKind[K, SK]] =
        Between(this, start, end, false)

    def notBetween[R](start: R, end: R)(using a: ComparableValue[R], o: CompareOperation[T, R]): Expr[Boolean, ResultKind[K, ValueKind]] =
        Between(this, a.asExpr(start), a.asExpr(end), true)

    def notBetween[S, SK <: ExprKind, E, EK <: ExprKind](start: Expr[S, SK], end: Expr[E, EK])(using CompareOperation[T, S], KindOperation[K, SK], CompareOperation[T, E], KindOperation[K, EK]): Expr[Boolean, ResultKind[K, SK]] =
        Between(this, start, end, true)

    @targetName("plus")
    def +[R: Number](value: R)(using n: Number[T], a: AsSqlExpr[R]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
        Binary(this, Plus, Literal(value, a))

    @targetName("plus")
    def +[R: Number, RK <: ExprKind](that: Expr[R, RK])(using Number[T], KindOperation[K, RK]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Plus, that)

    @targetName("plus")
    def +(interval: TimeInterval)(using DateTime[T]): Expr[Wrap[T, Option], ResultKind[K, ValueKind]] =
        Binary(this, Plus, Interval(interval.value, interval.unit))

    @targetName("minus")
    def -[R: Number](value: R)(using n: Number[T], a: AsSqlExpr[R]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
        Binary(this, Minus, Literal(value, a))

    @targetName("minus")
    def -[R, RK <: ExprKind](that: Expr[R, RK])(using m: MinusOperation[T, R], k: KindOperation[K, RK]): Expr[m.R, ResultKind[K, RK]] =
        Binary(this, Minus, that)

    @targetName("minus")
    def -(interval: TimeInterval)(using DateTime[T]): Expr[Wrap[T, Option], ResultKind[K, ValueKind]] =
        Binary(this, Minus, Interval(interval.value, interval.unit))

    @targetName("times")
    def *[R: Number](value: R)(using n: Number[T], a: AsSqlExpr[R]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
        Binary(this, Times, Literal(value, a))

    @targetName("times")
    def *[R: Number, RK <: ExprKind](that: Expr[R, RK])(using Number[T], KindOperation[K, RK]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Times, that)

    @targetName("div")
    def /[R: Number](value: R)(using n: Number[T], a: AsSqlExpr[R]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
        Binary(this, Div, Literal(value, a))

    @targetName("div")
    def /[R: Number, RK <: ExprKind](that: Expr[R, RK])(using Number[T], KindOperation[K, RK]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Div, that)

    @targetName("mod")
    def %[R: Number](value: R)(using n: Number[T], a: AsSqlExpr[R]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
        Binary(this, Mod, Literal(value, a))

    @targetName("mod")
    def %[R: Number, RK <: ExprKind](that: Expr[R, RK])(using Number[T], KindOperation[K, RK]): Expr[Option[BigDecimal], ResultKind[K, RK]] =
        Binary(this, Mod, that)

    @targetName("positive")
    def unary_+(using Number[T]): Expr[T, ResultKind[K, ValueKind]] = Unary(this, Positive)

    @targetName("negative")
    def unary_-(using Number[T]): Expr[T, ResultKind[K, ValueKind]] = Unary(this, Negative)

    @targetName("and")
    inline def &&[R, RK <: ExprKind](that: Expr[R, RK])(using KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        inline (erasedValue[T], erasedValue[R]) match
            case (_: Boolean, _: Boolean) => 
            case _ => error("The parameters for AND must be of Boolean type.")
        Binary(this, And, that)

    @targetName("or")
    inline def ||[R, RK <: ExprKind](that: Expr[R, RK])(using KindOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        inline (erasedValue[T], erasedValue[R]) match
            case (_: Boolean, _: Boolean) => 
            case _ => error("The parameters for OR must be of Boolean type.")
        Binary(this, Or, that)

    @targetName("not")
    inline def unary_! : Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: Boolean => 
            case _ => error("The parameters for NOT must be of Boolean type.")
        Unary(this, Not)

    inline def like(value: String): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: String =>
            case _: Option[String] =>
            case _ => error("The parameters for LIKE must be of String type.")
        Binary(this, Like, Literal(value, summon[AsSqlExpr[String]]))

    inline def like[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        inline erasedValue[T] match
            case _: String =>
            case _: Option[String] =>
            case _ => error("The parameters for LIKE must be of String type.")
        inline erasedValue[R] match
            case _: String =>
            case _: Option[String] =>
            case _ => error("The parameters for LIKE must be of String type.")
        Binary(this, Like, that)

    inline def notLike(value: String): Expr[Boolean, ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: String =>
            case _: Option[String] =>
            case _ => error("The parameters for LIKE must be of String type.")
        Binary(this, NotLike, Literal(value, summon[AsSqlExpr[String]]))

    inline def notLike[R, RK <: ExprKind](that: Expr[R, RK])(using CompareOperation[K, RK]): Expr[Boolean, ResultKind[K, RK]] =
        inline erasedValue[T] match
            case _: String =>
            case _: Option[String] =>
            case _ => error("The parameters for LIKE must be of String type.")
        inline erasedValue[R] match
            case _: String =>
            case _: Option[String] =>
            case _ => error("The parameters for LIKE must be of String type.")
        Binary(this, NotLike, that)

    inline def contains(value: String): Expr[Boolean, ResultKind[K, ValueKind]] =
        like("%" + value + "%")

    inline def startsWith(value: String): Expr[Boolean, ResultKind[K, ValueKind]] =
        like(value + "%")
    
    inline def endsWith(value: String): Expr[Boolean, ResultKind[K, ValueKind]] =
        like("%" + value)

    @targetName("json")
    inline def ->(n: Int): Expr[Option[Json], ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: Json =>
            case _: Option[Json] =>
            case _ => error("The first parameters for -> must be of Json type.")
        Binary(this, Json, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("json")
    inline def ->(n: String): Expr[Option[Json], ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: Json =>
            case _: Option[Json] =>
            case _ => error("The first parameters for -> must be of Json type.")
        Binary(this, Json, Literal(n, summon[AsSqlExpr[String]]))

    @targetName("jsonText")
    inline def ->>(n: Int): Expr[Option[String], ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: Json =>
            case _: Option[Json] =>
            case _ => error("The first parameters for -> must be of Json type.")
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("jsonText")
    inline def ->>(n: String): Expr[Option[String], ResultKind[K, ValueKind]] =
        inline erasedValue[T] match
            case _: Json =>
            case _: Option[Json] =>
            case _ => error("The first parameters for -> must be of Json type.")
        Binary(this, JsonText, Literal(n, summon[AsSqlExpr[String]]))

object Expr:
    extension [T](expr: Expr[T, ?])
        private[sqala] def asSqlExpr: SqlExpr = expr match
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
            case Unary(expr, op) =>
                SqlExpr.Unary(expr.asSqlExpr, op)
            case SubQuery(query) => SqlExpr.SubQuery(query)
            case Func(name, args, distinct, orderBy, withinGroupOrderBy, filter) =>
                SqlExpr.Func(name, args.map(_.asSqlExpr), distinct, Map(), orderBy.map(_.asSqlOrderBy), withinGroupOrderBy.map(_.asSqlOrderBy), filter.map(_.asSqlExpr))
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

    extension [T](expr: Expr[T, ColumnKind])
        @targetName("to")
        def :=(value: T)(using a: AsSqlExpr[T]): UpdatePair = expr match
            case Column(_, columnName) =>
                UpdatePair(columnName, Literal(value, a))

        @targetName("to")
        def :=[R, K <: ExprKind](updateExpr: Expr[R, K])(using CompareOperation[T, R], KindOperation[ColumnKind, K]): UpdatePair = expr match
            case Column(_, columnName) =>
                UpdatePair(columnName, updateExpr)

    extension [T](expr: Expr[T, AggKind])
        inline def filter[RK <: ExprKind](cond: Expr[Boolean, RK]): Expr[T, AggKind] = 
            inline erasedValue[RK] match
                case _: SimpleKind =>
                case _ => error("The parameters for FILTER cannot contain aggregate functions or window functions.")
            expr match
                case f: Func[?, ?] => f.copy(filter = Some(cond))

        inline def over[P, O](partitionBy: P = EmptyTuple, orderBy: O = EmptyTuple, frame: Option[SqlWindowFrame] | SqlWindowFrame = None): Expr[T, WindowKind] =
            inline erasedValue[CheckOverPartition[P]] match
                case _: false => 
                    error("The parameters for PARTITION BY cannot contain aggregate functions or window functions.")
                case _ =>
            inline erasedValue[CheckOverOrder[O]] match
                case _: false => 
                    error("The parameters for ORDER BY cannot contain aggregate functions or window functions or constants.")
                case _ =>
            val partition = partitionBy match
                case e: Expr[?, ?] => e :: Nil
                case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
            val order = orderBy match
                case o: OrderBy[?, ?] => o :: Nil
                case t: Tuple => t.toList.map(_.asInstanceOf[OrderBy[?, ?]])
            val frameClause = frame match
                case o: Option[?] => o
                case o: SqlWindowFrame => Some(o)
            Window(expr, partition, order, frameClause)

    extension [T, K <: ExprKind](expr: Expr[T, K])
        def asc: OrderBy[T, K] = OrderBy(expr, Asc, None)

        def desc: OrderBy[T, K] = OrderBy(expr, Desc, None)

        def ascNullsFirst: OrderBy[T, K] = OrderBy(expr, Asc, Some(First))

        def ascNullsLast: OrderBy[T, K] = OrderBy(expr, Asc, Some(Last))

        def descNullsFirst: OrderBy[T, K] = OrderBy(expr, Desc, Some(First))

        def descNullsLast: OrderBy[T, K] = OrderBy(expr, Desc, Some(Last))

case class OrderBy[T, K <: ExprKind](expr: Expr[?, ?], order: SqlOrderByOption, nullsOrder: Option[SqlOrderByNullsOption]):
    private[sqala] def asSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.asSqlExpr, Some(order), nullsOrder)

case class TimeInterval(value: Double, unit: SqlTimeUnit)

case class SubLinkItem[T](query: SqlQuery, linkType: SqlSubLinkType)

case class WindowFunc[T](
   name: String,
   args: List[Expr[?, ?]],
   distinct: Boolean = false,
   orderBy: List[OrderBy[?, ?]] = Nil
):
    inline def over[P, O](partitionBy: P = EmptyTuple, orderBy: O = EmptyTuple, frame: Option[SqlWindowFrame] | SqlWindowFrame = None): Expr[T, WindowKind] =
        inline erasedValue[CheckOverPartition[P]] match
            case _: false => 
                error("The parameters for PARTITION BY cannot contain aggregate functions or window functions.")
            case _ =>
        inline erasedValue[CheckOverOrder[O]] match
            case _: false => 
                error("The parameters for ORDER BY cannot contain aggregate functions or window functions or constants.")
            case _ =>
        val partition = partitionBy match
            case e: Expr[?, ?] => e :: Nil
            case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
        val order = orderBy match
            case o: OrderBy[?, ?] => o :: Nil
            case t: Tuple => t.toList.map(_.asInstanceOf[OrderBy[?, ?]])
        val frameClause = frame match
            case o: Option[?] => o
            case o: SqlWindowFrame => Some(o)
        val func = Expr.Func(name, args, distinct, this.orderBy)
        Expr.Window(func, partition, order, frameClause)