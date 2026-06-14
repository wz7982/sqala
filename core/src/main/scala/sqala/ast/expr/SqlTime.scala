package sqala.ast.expr

import sqala.ast.token.SqlCustomToken

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
     * Renders as the given `tokens` directly.
     */
    case Custom(tokens: List[SqlCustomToken])

/**
 * Interval field specifications used in `INTERVAL` literals.
 */
enum SqlIntervalField:
    /**
     * A range interval field from `start` to `end`.
     *
     * Renders as `unit TO unit` (e.g. `YEAR TO MONTH`).
     */
    case To(start: SqlTimeUnit, end: SqlTimeUnit)

    /**
     * A single interval field.
     *
     * Renders as the time unit (e.g. `DAY`).
     */
    case Single(unit: SqlTimeUnit)

/**
 * Units for time or date literal expressions.
 */
enum SqlTimeType:
    /**
     * `TIMESTAMP` literal, optionally with a time zone mode.
     *
     * Renders as `TIMESTAMP [WITH|WITHOUT TIME ZONE]`.
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