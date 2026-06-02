package sqala.ast.expr

/**
 * Data types used in `CAST` expressions and column definitions.
 */
enum SqlType:
    /**
     * Variable-length character string.
     *
     * Renders as `VARCHAR|VARCHAR(n)`.
     */
    case Varchar(maxLength: Option[Int])

    /**
     * 32-bit integer.
     *
     * Renders as `INT`.
     */
    case Int

    /**
     * 64-bit integer.
     *
     * Renders as `BIGINT`.
     */
    case Long

    /**
     * Single-precision floating point.
     *
     * Renders as `REAL`.
     */
    case Float

    /**
     * Double-precision floating point.
     *
     * Renders as `DOUBLE PRECISION`.
     */
    case Double

    /**
     * Fixed-point decimal number.
     *
     * Renders as `DECIMAL|DECIMAL(p, s)`.
     */
    case Decimal(precision: Option[(Int, Int)])

    /**
     * Date value (no time component).
     *
     * Renders as `DATE`.
     */
    case Date

    /**
     * Timestamp value, optionally with time zone.
     *
     * Renders as `TIMESTAMP [WITH|WITHOUT TIME ZONE]`.
     */
    case Timestamp(mode: Option[SqlTimeZoneMode])

    /**
     * Time-of-day value, optionally with time zone.
     *
     * Renders as `TIME [WITH|WITHOUT TIME ZONE]`.
     */
    case Time(mode: Option[SqlTimeZoneMode])

    /**
     * JSON data type.
     *
     * Renders as `JSON`.
     */
    case Json

    /**
     * Boolean data type.
     *
     * Renders as `BOOLEAN`.
     */
    case Boolean

    /**
     * Interval data type.
     *
     * Renders as `INTERVAL`.
     */
    case Interval

    /**
     * Generic geometry type.
     *
     * Renders as `GEOMETRY`.
     */
    case Geometry

    /**
     * Point geometry type.
     *
     * Renders as `POINT`.
     */
    case Point

    /**
     * LineString geometry type.
     *
     * Renders as `LINESTRING`.
     */
    case LineString

    /**
     * Polygon geometry type.
     *
     * Renders as `POLYGON`.
     */
    case Polygon

    /**
     * MultiPoint geometry type.
     *
     * Renders as `MULTIPOINT`.
     */
    case MultiPoint

    /**
     * MultiLineString geometry type.
     *
     * Renders as `MULTILINESTRING`.
     */
    case MultiLineString

    /**
     * MultiPolygon geometry type.
     *
     * Renders as `MULTIPOLYGON`.
     */
    case MultiPolygon

    /**
     * GeometryCollection type.
     *
     * Renders as `GEOMETRYCOLLECTION`.
     */
    case GeometryCollection

    /**
     * Array type with an element type.
     *
     * Renders as `type[]`.
     */
    case Array(`type`: SqlType)

    /**
     * A custom type with a free-form name.
     *
     * Renders as the given `type` string directly.
     */
    case Custom(`type`: String)