package sqala.metadata

import sqala.ast.expr.{SqlExpr, SqlType}

/**
 * Allows a user-defined type to be used as a value in sqala expressions
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
 */
trait CustomField[T, R](using a: AsSqlExpr[R]) extends AsSqlExpr[T]:
    /**
     * Extracts the underlying value from the custom type.
     */
    def toValue(x: T): R

    /**
     * Wraps the underlying value into the custom type.
     */
    def fromValue(x: R): T

    def asSqlExpr(x: T, inlined: Boolean): SqlExpr =
        a.asSqlExpr(toValue(x), inlined)

    def sqlType: SqlType =
        a.sqlType

object CustomField:
    /**
     * Derives a `CustomField` instance for a parameterless Scala 3 enum,
     * reading the database values from `@enumValue` or `@enumName`.
     *
     * The storage type must be written explicitly, because it decides which
     * annotations are valid and which `JdbcDecoder` is used:
     *
     * {{{
     * enum DataState:
     *     @enumValue(1) case Active
     *     @enumValue(0) case Deleted
     *
     * object DataState:
     *     given CustomField[DataState, Int] = CustomField.derived
     * }}}
     *
     * {{{
     * enum Status:
     *     @enumName("ACTIVE") case Active
     *     case Deleted
     *
     * object Status:
     *     given CustomField[Status, String] = CustomField.derived
     * }}}
     *
     * The macro rejects:
     *
     *   - enums with type parameters or case parameters
     *   - integer-mapped cases without `@enumValue`
     *   - annotations that do not match the storage type
     *   - duplicate database values
     *   - non-literal annotation arguments
     *
     * Decoding an unknown database value throws `EnumDecodeException`.
     */
    inline def derived[T, R]: CustomField[T, R] =
        ${ CustomFieldMacro.derive[T, R] }