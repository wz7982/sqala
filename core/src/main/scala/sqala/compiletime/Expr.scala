package sqala.compiletime

import sqala.ast.expr.*
import sqala.ast.order.{SqlOrderBy, SqlOrderByOption}
import sqala.ast.statement.SqlSelectItem
import sqala.compiletime.statement.query.Query

import java.util.Date

sealed trait Expr[T, TableNames <: Tuple]:
    def toSqlExpr: SqlExpr

object Expr:
    given intOperator: NumberOperator[Int] with {}

    given longOperator: NumberOperator[Long] with {}

    given floatOperator: NumberOperator[Float] with {}

    given doubleOperator: NumberOperator[Double] with {}

    given bigDecimalOperator: NumberOperator[BigDecimal] with {}

    given optionIntOperator: NumberOperator[Option[Int]] with {}

    given optionLongOperator: NumberOperator[Option[Long]] with {}

    given optionFloatOperator: NumberOperator[Option[Float]] with {}

    given optionDoubleOperator: NumberOperator[Option[Double]] with {}

    given optionBigDecimalOperator: NumberOperator[Option[BigDecimal]] with {}

    given stringOperator: StringOperator[String] with {}

    given optionStringOperator: StringOperator[Option[String]] with {}

    given booleanOperator: BooleanOperator with {}

    given dateOperator: DateOperator[Date] with {}

    given optionDateOperator: DateOperator[Option[Date]] with {}

case class Literal[T](value: T)(using d: AsSqlExpr[T]) extends Expr[T, EmptyTuple]:
    override def toSqlExpr: SqlExpr = d.asSqlExpr(value)

case object Null extends Expr[Nothing, EmptyTuple]:
    override def toSqlExpr: SqlExpr = SqlExpr.Null

case class Binary[T, TableNames <: Tuple](left: Expr[?, ?], op: SqlBinaryOperator, right: Expr[?, ?]) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = this match
        case Binary(left, SqlBinaryOperator.Equal, Literal(None)) => SqlExpr.Binary(left.toSqlExpr, SqlBinaryOperator.Is, SqlExpr.Null)
        case Binary(left, SqlBinaryOperator.NotEqual, Literal(None)) => SqlExpr.Binary(left.toSqlExpr, SqlBinaryOperator.IsNot, SqlExpr.Null)
        case Binary(_, _, Literal(None)) => SqlExpr.BooleanLiteral(false)
        case _ => SqlExpr.Binary(left.toSqlExpr, op, right.toSqlExpr)

case class Unary[T, TableNames <: Tuple](expr: Expr[?, ?], op: SqlUnaryOperator) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Unary(expr.toSqlExpr, op)

case class Column[T, TableName <: String, IdentName <: String](tableName: TableName, columnName: String, identName: IdentName) extends Expr[T, Tuple1[TableName]]:
    override def toSqlExpr: SqlExpr = SqlExpr.Column(Some(tableName), columnName)

case class PrimaryKey[T, TableName <: String, IdentName <: String](tableName: TableName, columnName: String, identName: IdentName) extends Expr[T, Tuple1[TableName]]:
    override def toSqlExpr: SqlExpr = SqlExpr.Column(Some(tableName), columnName)

case class SubQuery[T](query: Query[?, ?]) extends Expr[T, EmptyTuple]:
    override def toSqlExpr: SqlExpr = SqlExpr.SubQuery(query.ast)

case class Func[T](name: String, args: List[Expr[?, ?]]) extends Expr[T, EmptyTuple]:
    override def toSqlExpr: SqlExpr = SqlExpr.Func(name, args.map(_.toSqlExpr))

case class Agg[T, TableNames <: Tuple](name: String, args: List[Expr[?, ?]], distinct: Boolean, orders: List[OrderBy]) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Agg(name, args.map(_.toSqlExpr), distinct, Map(), orders.map(_.toSqlOrderBy))

    def over: Window[T, TableNames] = Window(this, Nil, Nil, None)

case class Case[T, TableNames <: Tuple](branches: List[CaseBranch[?]], default: Expr[?, ?]) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Case(branches.map(_.toSqlCase), default.toSqlExpr)

case class Vector[T, TableNames <: Tuple](items: List[Expr[?, ?]]) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Vector(items.map(_.toSqlExpr))

case class In[T, TableNames <: Tuple](expr: Expr[?, ?], inExpr: Expr[?, ?], not: Boolean) extends Expr[Boolean, TableNames]:
    override def toSqlExpr: SqlExpr = this match
        case In(_, Vector(Nil), false) => SqlExpr.BooleanLiteral(false)
        case In(_, Vector(Nil), true) => SqlExpr.BooleanLiteral(true)
        case _ => SqlExpr.In(expr.toSqlExpr, inExpr.toSqlExpr, not)

case class Between[T, TableNames <: Tuple](expr: Expr[?, ?], start: Expr[?, ?], end: Expr[?, ?], not: Boolean) extends Expr[Boolean, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Between(expr.toSqlExpr, start.toSqlExpr, end.toSqlExpr, not)

case object AllColumn extends Expr[Nothing, EmptyTuple]:
    override def toSqlExpr: SqlExpr = SqlExpr.AllColumn(None)

case class Cast[T, TableNames <: Tuple](expr: Expr[?, ?], castType: String) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Cast(expr.toSqlExpr, castType)

case class Window[T, TableNames <: Tuple](agg: Agg[?, ?], partitions: List[Expr[?, ?]], orders: List[OrderBy], frame: Option[SqlWindowFrame]) extends Expr[T, TableNames]:
    override def toSqlExpr: SqlExpr = SqlExpr.Window(agg.toSqlExpr, partitions.map(_.toSqlExpr), orders.map(_.toSqlOrderBy), frame)

    infix def partitionBy(expr: Expr[?, ?]): Window[T, TableNames] = Window(this.agg, expr :: Nil, this.orders, this.frame)

    infix def partitionBy[P <: Tuple](items: P): Window[T, TableNames] =
        val partitions = items.toList.map:
            case e: Expr[_, _] => e
        Window(this.agg, partitions, this.orders, this.frame)

    infix def orderBy(order: OrderBy): Window[T, TableNames] = Window(this.agg, this.partitions, order :: Nil, this.frame)

    infix def orderBy[O <: Tuple](items: O): Window[T, TableNames] =
        val orders = items.toList.map:
            case o: OrderBy => o
        Window(this.agg, this.partitions, orders, this.frame)

    infix def rowsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): Window[T, TableNames] =
        copy(frame = Some(SqlWindowFrame.Rows(start, end)))

    infix def rangeBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): Window[T, TableNames] =
        copy(frame = Some(SqlWindowFrame.Range(start, end)))

case class SubQueryPredicate[T](query: Query[?, ?], predicate: SqlSubQueryPredicate) extends Expr[T, EmptyTuple]:
    override def toSqlExpr: SqlExpr = SqlExpr.SubQueryPredicate(query.ast, predicate)

case class OrderBy(expr: Expr[?, ?], order: SqlOrderByOption):
    def toSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.toSqlExpr, Some(order))

case class CaseBranch[T](expr: Expr[?, ?], thenExpr: Expr[?, ?]):
    def toSqlCase: SqlCase = SqlCase(expr.toSqlExpr, thenExpr.toSqlExpr)

case class SelectItem[T, TableNames <: Tuple, Alias <: String](expr: Expr[?, ?], alias: Alias):
    def toSqlSelectItem: SqlSelectItem = SqlSelectItem(expr.toSqlExpr, Some(alias))

trait ExprNumber[T]

object ExprNumber:
    given exprNumberInt: ExprNumber[Int] with {}

    given exprNumberLong: ExprNumber[Long] with {}

    given exprNumberFloat: ExprNumber[Float] with {}

    given exprNumberDouble: ExprNumber[Double] with {}

    given exprNumberBigDecimal: ExprNumber[BigDecimal] with {}

    given exprNumberOption[T](using ExprNumber[T]): ExprNumber[Option[T]] with {}