package sqala.test

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import sqala.ast.expr.SqlTimeUnit
import sqala.dialect.PostgresqlDialect
import sqala.metadata.{CustomField, Geometry, GeometryCollection, Interval, Json, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, autoInc, primaryKey}
import sqala.static.dsl.{Expr, ExprKind}
import sqala.static.dsl.statement.query.Query

case class User(id: Int, name: String, age: Int)
case class Post(id: Int, userId: Int, title: String, viewCount: Int)
case class Wide(id: Int, name: String, flag: Boolean, birth: LocalDate, score: Option[Long])
case class Node(id: Int, parentId: Option[Int], name: String)
case class Account(@primaryKey @autoInc id: Int, name: String, balance: Long, memo: Option[String])
case class Link(@primaryKey src: Int, @primaryKey dst: Int, label: String)
case class Marker(@primaryKey code: Int)
case class Profile(@primaryKey id: Int, nickname: Option[String], score: Option[Long])

case class Nums(i: Int, l: Long, f: Float, d: Double, bd: BigDecimal, oi: Option[Int], od: Option[Double])
case class Texts(s: String, b: Boolean, os: Option[String], ob: Option[Boolean])
case class Times(
    dt: LocalDate,
    ts: OffsetDateTime,
    lts: LocalDateTime,
    tm: OffsetTime,
    ltm: LocalTime,
    iv: Interval,
    odt: Option[LocalDate],
    ots: Option[OffsetDateTime],
    olts: Option[LocalDateTime],
    otm: Option[OffsetTime],
    oltm: Option[LocalTime],
    oiv: Option[Interval]
)
case class Extras(js: Json, geo: Point, arr: Array[Int], arrd: Array[Double], ojs: Option[Json], ogeo: Option[Point], oarr: Option[Array[Int]])

case class Cross(dt: LocalDate, ts: OffsetDateTime, tm: OffsetTime, s: String, odt: Option[LocalDate], os: Option[String])

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

enum Status:
    case Active, Inactive

object Status:
    given CustomField[Status, String] with
        def toValue(x: Status): String = x.toString

        def fromValue(x: String): Status = Status.valueOf(x)

case class Customs(st: Status, ost: Option[Status])

object StaticFixture:
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
    val json: Json = Json("{}")
    val point: Point = Point("POINT(1 1)", 4326)