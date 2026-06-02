package sqala.metadata

import sqala.ast.expr.SqlTimeUnit

import java.time.*

/**
 * Marker trait that constrains an expression type to numeric types.
 */
trait SqlNumber[T]

object SqlNumber:
    given int: SqlNumber[Int]()

    given long: SqlNumber[Long]()

    given float: SqlNumber[Float]()

    given double: SqlNumber[Double]()

    given decimal: SqlNumber[BigDecimal]()

    given option[T: SqlNumber]: SqlNumber[Option[T]]()

/**
 * Marker trait that constrains an expression type to date-time types.
 */
trait SqlDateTime[T]

object SqlDateTime:
    given localDate: SqlDateTime[LocalDate]()

    given localDateTime: SqlDateTime[LocalDateTime]()

    given offsetDateTime: SqlDateTime[OffsetDateTime]()

    given option[T: SqlDateTime]: SqlDateTime[Option[T]]()

/**
 * Marker trait that constrains an expression type to time types.
 */
trait SqlTime[T]

object SqlTime:
    given localTime: SqlTime[LocalTime]()

    given offsetTime: SqlTime[OffsetTime]()

    given option[T: SqlTime]: SqlTime[Option[T]]()

/**
 * Marker trait that constrains an expression type to `String`.
 */
trait SqlString[T]

object SqlString:
    given string: SqlString[String]()

    given option[T: SqlString]: SqlString[Option[T]]()

/**
 * Marker trait that constrains an expression type to `Boolean`.
 */
trait SqlBoolean[T]

object SqlBoolean:
    given boolean: SqlBoolean[Boolean]()

    given option[T: SqlBoolean]: SqlBoolean[Option[T]]()

/**
 * Marker trait that constrains an expression type to `Json`.
 */
trait SqlJson[T]

object SqlJson:
    given json: SqlJson[Json]()

    given option[T: SqlJson]: SqlJson[Option[T]]()

/**
 * Marker trait that constrains an expression type to spatial geometry types.
 */
trait SqlGeometry[T]

object SqlGeometry:
    given geometry: SqlGeometry[Geometry]()

    given point: SqlGeometry[Point]()

    given lineString: SqlGeometry[LineString]()

    given polygon: SqlGeometry[Polygon]()

    given multiPoint: SqlGeometry[MultiPoint]()

    given multiLineString: SqlGeometry[MultiLineString]()

    given multiPolygon: SqlGeometry[MultiPolygon]()

    given geometryCollection: SqlGeometry[GeometryCollection]()

    given option[T: SqlGeometry]: SqlGeometry[Option[T]]()

/**
 * Marker trait that constrains an expression type to `Interval`.
 */
trait SqlInterval[T]

object SqlInterval:
    given interval: SqlInterval[Interval]()

    given option[T: SqlInterval]: SqlInterval[Option[T]]()

/**
 * Opaque string type representing a JSON value.
 */
opaque type Json = String

object Json:
    def apply(value: String): Json = value

/**
 * An interval quantity paired with a time unit.
 *
 * `value` is the interval string (e.g. `"1"`), `unit` is the time unit
 * (e.g. `DAY`, `YEAR TO MONTH`).
 */
final case class Interval(value: String, unit: SqlTimeUnit)    

/**
 * A spatial geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class Geometry(value: String, srid: Int)

/**
 * A point geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class Point(value: String, srid: Int)

/**
 * A line string geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class LineString(value: String, srid: Int)

/**
 * A polygon geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class Polygon(value: String, srid: Int)

/**
 * A multi-point geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class MultiPoint(value: String, srid: Int)

/**
 * A multi-line string geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class MultiLineString(value: String, srid: Int)

/**
 * A multi-polygon geometry value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class MultiPolygon(value: String, srid: Int)

/**
 * A geometry collection value. `value` is a WKT string, `srid` is the
 * spatial reference system identifier.
 */
final case class GeometryCollection(value: String, srid: Int)