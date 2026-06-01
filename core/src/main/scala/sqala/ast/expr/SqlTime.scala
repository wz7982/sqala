package sqala.ast.expr

/**
 * Time units used in `EXTRACT` and interval expressions.
 */
enum SqlTimeUnit:
    /**
     * Year.
     *
     * Renders as `YEAR`.
     */
    case Year

    /**
     * Month.
     *
     * Renders as `MONTH`.
     */
    case Month

    /**
     * Day.
     *
     * Renders as `DAY`.
     */
    case Day

    /**
     * Hour.
     *
     * Renders as `HOUR`.
     */
    case Hour

    /**
     * Minute.
     *
     * Renders as `MINUTE`.
     */
    case Minute

    /**
     * Second.
     *
     * Renders as `SECOND`.
     */
    case Second

    /**
     * A custom time unit with a free-form name.
     *
     * Renders as the given `unit` string directly.
     *
     * @param unit the unit text.
     */
    case Custom(unit: String)

/**
 * Interval field specifications used in `INTERVAL` literals.
 */
enum SqlIntervalField:
    /**
     * A range interval field from `start` to `end`.
     *
     * Renders as `start TO end` (e.g. `YEAR TO MONTH`).
     *
     * @param start the start unit.
     * @param end the end unit.
     */
    case To(start: SqlTimeUnit, end: SqlTimeUnit)

    /**
     * A single interval field.
     *
     * Renders as the time unit (e.g. `DAY`).
     *
     * @param unit the time unit.
     */
    case Single(unit: SqlTimeUnit)

/**
 * Units for time / date literal expressions.
 */
enum SqlTimeLiteralUnit:
    /**
     * `TIMESTAMP` literal, optionally with a time zone mode.
     *
     * Renders as `TIMESTAMP [WITH|WITHOUT TIME ZONE]`.
     *
     * @param mode optional time zone mode (WITH or WITHOUT).
     */
    case Timestamp(mode: Option[SqlTimeZoneMode])

    /**
     * `DATE` literal.
     *
     * Renders as `DATE`.
     */
    case Date

    /**
     * `TIME` literal, optionally with a time zone mode.
     *
     * Renders as `TIME [WITH|WITHOUT TIME ZONE]`.
     *
     * @param mode optional time zone mode (WITH or WITHOUT).
     */
    case Time(mode: Option[SqlTimeZoneMode])

/**
 * Time zone mode for temporal types.
 */
enum SqlTimeZoneMode:
    /**
     * With time zone.
     *
     * Renders as `WITH TIME ZONE`.
     */
    case With

    /**
     * Without time zone.
     *
     * Renders as `WITHOUT TIME ZONE`.
     */
    case Without