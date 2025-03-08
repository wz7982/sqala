package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.param.SqlParam
import sqala.ast.statement.SqlQuery
import sqala.common.*
import sqala.static.statement.dml.UpdatePair

import scala.annotation.targetName

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
        c: Compare[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, Equal, a.asExpr(that))

    @targetName("eq")
    def ==[R](item: SubLinkItem[R])(using
        Compare[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, Equal, SubLink(item.query, item.linkType))

    @targetName("ne")
    def !=[R](that: R)(using
        a: AsExpr[R],
        c: Compare[Unwrap[T, Option], Unwrap[a.R, Option]]
    ): Expr[Boolean] =
        Binary(this, NotEqual, a.asExpr(that))

    @targetName("ne")
    def !=[R](item: SubLinkItem[R])(using
        Compare[Unwrap[T, Option],
        Unwrap[R, Option]]
    ): Expr[Boolean] =
        Binary(this, NotEqual, SubLink(item.query, item.linkType))

    infix def over(overValue: OverValue): Expr[T] =
        Expr.Window(this, overValue.partitionBy, overValue.sortBy, overValue.frame)

    infix def over(overValue: Unit): Expr[T] =
        Expr.Window(this, Nil, Nil, None)

    @targetName("to")
    def :=[R: AsExpr as a](updateExpr: R)(using
        Compare[T, a.R]
    ): UpdatePair = this match
        case Column(_, columnName) =>
            UpdatePair(columnName, a.asExpr(updateExpr).asSqlExpr)
        case _ => throw MatchError(this)

object Expr:
    extension [T](expr: Expr[T])
        private[sqala] def asSqlExpr: SqlExpr = expr match
            case Literal(v, a) => a.asSqlExpr(v)
            case Column(tableName, columnName) =>
                SqlExpr.Column(Some(tableName), columnName)
            case Binary(left, SqlBinaryOperator.Equal, right) if right.asSqlExpr == SqlExpr.Null =>
                SqlExpr.NullTest(left.asSqlExpr, false)
            case Binary(left, SqlBinaryOperator.NotEqual, right) if right.asSqlExpr == SqlExpr.Null =>
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