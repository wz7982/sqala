package sqala.test

import sqala.dialect.PostgresqlDialect
import sqala.metadata.{Geometry, GeometryCollection, Interval, Json, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, autoInc, primaryKey}
import sqala.ast.expr.SqlTimeUnit
import sqala.static.dsl.{Expr, ExprKind}
import sqala.static.dsl.statement.query.Query

case class Person(id: Int, name: String, age: Int)
case class Doc(id: Int, body: String, meta: Json, ometa: Option[Json], obody: Option[String])
case class Num(i: Int, l: Long, d: Double, bd: BigDecimal, oi: Option[Int])
case class Arr(id: Int, tags: Array[String], nums: Array[Int], onums: Option[Array[Int]])
case class Event(id: Int, accountId: Int, amount: Long, kind: String, ts: Long)
case class Friendship(personAId: Int, personBId: Int, since: Long)
case class Account(@primaryKey @autoInc id: Int, name: String, balance: Long)

case class Geo(
    g: Geometry,
    p: Point,
    ls: LineString,
    pg: Polygon,
    mp: MultiPoint,
    mls: MultiLineString,
    mpg: MultiPolygon,
    gc: GeometryCollection,
    op: Option[Point]
)

object ExtFixture:
    def lines(xs: String*): String = xs.mkString("\n")

    def sql(q: Query[?, ?, ?, ?]): String = q.sql(PostgresqlDialect)

    /** Checks the inferred value type of an expression, leaving its kind free. */
    class Expect[T]:
        def apply[K <: ExprKind](e: Expr[T, K]): Expr[T, K] = e

    def expect[T]: Expect[T] = new Expect

    /** Checks the inferred result type of a query. */
    def result[T](q: Query[T, ?, ?, ?]): Query[T, ?, ?, ?] = q

    /** The diagnostic messages of a compiler output, in reporting order. */
    def messages(compilerOutput: String): List[String] =
        compilerOutput.linesIterator
            .filter(_.startsWith("error: "))
            .map(_.stripPrefix("error: "))
            .toList

    val interval: Interval = Interval("1", SqlTimeUnit.Day)
    val point: Point = Point("POINT(1 1)", 4326)