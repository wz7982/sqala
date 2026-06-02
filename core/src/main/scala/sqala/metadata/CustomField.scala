package sqala.metadata

import sqala.ast.expr.{SqlExpr, SqlType}

/**
 * Allows a user-defined Scala type to be used as a value in sqala expressions
 * by mapping it to a supported type with a built-in `AsSqlExpr` instance.
 *
 * Implement `toValue` and `fromValue` to define the bidirectional conversion.
 *
 * {{{
 * enum Status:
 *     case Active, Inactive
 *
 * object Status:
 *     given CustomField[Status, String] with
 *         def toValue(x: Status): String = x.toString
 * 
 *         def fromValue(x: String): Status = Status.valueOf(x)
 * }}}
 *
 * @tparam T the custom Scala type.
 * @tparam R the underlying type with an `AsSqlExpr` instance.
 */
trait CustomField[T, R](using a: AsSqlExpr[R]) extends AsSqlExpr[T]:
    /**
     * Extracts the underlying value from the custom type.
     *
     * @param x the custom type value.
     */
    def toValue(x: T): R

    /**
     * Wraps the underlying value into the custom type.
     *
     * @param x the underlying type value.
     */
    def fromValue(x: R): T

    def asSqlExpr(x: T): SqlExpr =
        a.asSqlExpr(toValue(x))

    def sqlType: SqlType =
        a.sqlType