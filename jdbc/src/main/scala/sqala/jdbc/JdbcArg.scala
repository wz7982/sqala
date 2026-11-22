package sqala.jdbc

import java.sql.PreparedStatement

/**
 * A parameter value paired with the `JdbcEncoder` selected for its
 * static type.
 *
 * Both the constructor and the encoder are public so custom printers can
 * create arguments directly when they need to bind values themselves.
 */
final class JdbcArg[T](
    val value: T,
    val encoder: JdbcEncoder[T]
):
    /**
     * Binds this argument to the `index`‑th parameter of `stmt`.
     */
    def bind(stmt: PreparedStatement, index: Int): Unit =
        encoder.encode(stmt, index, value)

object JdbcArg:
    /**
     * Creates an argument, resolving its encoder from the static type of
     * `value`.
     */
    def apply[T](value: T)(using e: JdbcEncoder[T]): JdbcArg[T] =
        new JdbcArg(value, e)